#!/usr/bin/env python3

"""
Ceph balancer.

(c) 2020 Jonas Jelten <jj@sft.lol>
GPLv3 or later
"""

# some future TODOs:
# also consider the device's relative PG count
# maximum movement limits
# recommendations for pg num
# respect OMAP_BYTES and OMAP_KEYS for a pg
# don't touch a pool with decreasing pg_num

# even "better" algorithm:
# get osdmap and crushmap
# calculate constraints weighted by device
# get current utilization weighted by device
# create z3 equation using these constraints
# transform result to upmap items


import argparse
import json
import logging
import shlex
import statistics
import subprocess
from collections import defaultdict
from functools import lru_cache
from pprint import pformat


cli = argparse.ArgumentParser()

cli.add_argument("-v", "--verbose", action="count", default=0,
                 help="increase program verbosity")
cli.add_argument("-q", "--quiet", action="count", default=0,
                 help="decrease program verbosity")

sp = cli.add_subparsers(dest='mode')
sp.required=True
showsp = sp.add_parser('show')
showsp.add_argument('--sort-shardsize', action='store_true',
                    help="sort the pool overview by shardsize")
showsp.add_argument('--osds', action='store_true',
                    help="show info about all the osds instead of just the pool overview")
showsp.add_argument('--format', choices=['plain', 'json'], default='plain',
                    help="output formatting: plain or json. default: %(default)s")
showsp.add_argument('--pgstate', choices=['up', 'acting'], default='acting',
                    help="which PG state to consider: up (planned) or acting (active). default: %(default)s")
showsp.add_argument('--per-pool-count', action='store_true',
                    help="in text formatting mode, show how many pgs for each pool are mapped")
showsp.add_argument('--normalize-pg-count', action='store_true',
                    help="normalize the pg count by disk size")
showsp.add_argument('--sort-pg-count', type=int,
                    help="sort osds by pg count of given pool id")
showsp.add_argument('--sort-utilization', action='store_true',
                    help="sort osds by utilization")
showsp.add_argument('--use-weighted-utilization', action='store_true',
                    help="calculate osd utilization by weighting device size")
showsp.add_argument('--use-shardsize-sum', action='store_true',
                    help="calculate osd utilization by adding all PG shards on it")
showsp.add_argument('--osd-fill-min', type=int, default=0,
                    help='minimum fill %% to show an osd, default: %(default)s%%')

sp.add_parser('showremapped')

balancep = sp.add_parser('balance')
balancep.add_argument('--max-pg-moves', '-m', type=int, default=10,
                      help='maximum number of pg movements to find, default: %(default)s')
balancep.add_argument('--only-poolid',
                      help='comma separated list of pool ids to consider for balancing')
balancep.add_argument('--only-crushclass',
                      help='comma separated list of crush classes to balance')

args = cli.parse_args()


def log_setup(setting, default=1):
    """
    Perform setup for the logger.
    Run before any logging.log thingy is called.

    if setting is 0: the default is used, which is WARNING.
    else: setting + default is used.
    """

    levels = (logging.ERROR, logging.WARNING, logging.INFO,
              logging.DEBUG, logging.NOTSET)

    factor = clamp(default + setting, 0, len(levels) - 1)
    level = levels[factor]

    logging.basicConfig(level=level, format="[%(asctime)s] %(message)s")
    logging.captureWarnings(True)


def clamp(number, smallest, largest):
    """ return number but limit it to the inclusive given value range """
    return max(smallest, min(number, largest))


log_setup(args.verbose - args.quiet)


def jsoncall(cmd, swallow_stderr=False):
    if not isinstance(cmd, list):
        raise ValueError("need cmd as list")
    stderrval = subprocess.DEVNULL if swallow_stderr else None
    rawdata = subprocess.check_output(cmd, stderr=stderrval)
    return json.loads(rawdata.decode())


def pprintsize(size_bytes, commaplaces=1):
    prefixes = ((1, 'K'), (2, 'M'), (3, 'G'), (4, 'T'), (5, 'E'), (6, 'Z'))
    for exp, name in prefixes:
        if abs(size_bytes) >= 1024 ** exp and abs(size_bytes) < 1024 ** (exp + 1):
            new_size = size_bytes / 1024 ** exp
            fstring = "%%.%df%%s" % commaplaces
            return fstring % (new_size, name)

    return "%.1fB" % size_bytes


# this is shitty: this whole script depends on these outputs,
# but they might be inconsistent, if the cluster had changes
# between calls....

# ceph pg dump always echoes "dumped all" on stderr, silence that.
pg_dump = jsoncall("ceph pg dump --format json".split(), swallow_stderr=True)
osd_dump = jsoncall("ceph osd dump --format json".split())
osd_df_dump = jsoncall("ceph osd df --format json".split())
df_dump = jsoncall("ceph df detail --format json".split())
pool_dump = jsoncall("ceph osd pool ls detail --format json".split())
crush_dump = jsoncall("ceph osd crush dump --format json".split())
crush_classes = jsoncall("ceph osd crush class ls --format json".split())


pools = dict()                        # poolid => props
poolnames = dict()                    # poolname => poolid
crushrules = dict()                   # ruleid => props
crushclassmembers = defaultdict(set)  # crushclass => osdidset
crushclasses = dict()                 # osdid => crushclass
maxpoolnamelen = 0
maxcrushclasslen = 0


for crush_class in crush_classes:
    class_osds = jsoncall(f"ceph osd crush class ls-osd {crush_class} --format json".split())

    crushclassmembers[crush_class].update(class_osds)
    for osdid in class_osds:
        crushclasses[osdid] = crush_class

    if len(crush_class) > maxcrushclasslen:
        maxcrushclasslen = len(crush_class)


for pool in osd_dump["pools"]:
    id = pool["pool"]
    name = pool["pool_name"]

    pools[id] = {
        'name': name,
        'crush_rule': pool["crush_rule"],
        'pg_num': pool["pg_num"],  # current pgs before merge
        'pgp_num': pool["pg_placement_num"],  # actual placed pg count
        'pg_num_target': pool["pg_num_target"],  # target pg num
        'size': pool["size"],
        'min_size': pool["min_size"],
    }

    if len(name) > maxpoolnamelen:
        maxpoolnamelen = len(name)

    poolnames[name] = id


upmap_items = dict()
for upmap_item in osd_dump["pg_upmap_items"]:
    remaps = list()
    for remap in upmap_item["mappings"]:
        remaps.append((remap["from"], remap["to"]))

    upmap_items[upmap_item["pgid"]] = list(sorted(remaps))


ec_profiles = dict()
for ec_profile, ec_spec in osd_dump["erasure_code_profiles"].items():
    ec_profiles[ec_profile] = {
        "data_chunks": int(ec_spec["k"]),
        "coding_chunks": int(ec_spec["m"]),
    }


for pool in df_dump["pools"]:
    id = pool["id"]
    pools[id].update({
        "stored": pool["stats"]["stored"],  # actually stored data
        "objects": pool["stats"]["objects"],  # number of pool objects
        "used": pool["stats"]["bytes_used"],  # including redundant data
        "store_avail": pool["stats"]["max_avail"],  # available storage amount
        "percent_used": pool["stats"]["percent_used"],
        "quota_bytes": pool["stats"]["quota_bytes"],
        "quota_objects": pool["stats"]["quota_objects"],
    })


for pool in pool_dump:
    id = pool["pool_id"]
    ec_profile = pool["erasure_code_profile"]

    pg_shard_size_avg = pools[id]["stored"] / pools[id]["pg_num"]

    if ec_profile:
        pg_shard_size_avg /= ec_profiles[ec_profile]["data_chunks"]

    pools[id].update({
        "erasure_code_profile": ec_profile,
        "repl_type": "ec" if ec_profile else "repl",
        "pg_shard_size_avg": pg_shard_size_avg
    })


for rule in crush_dump["rules"]:
    id = rule['rule_id']
    name = rule['rule_name']
    steps = rule['steps']

    crushrules[id] = {
        'name': name,
        'steps': steps,
    }


# map osd -> pgs on it
osd_mappings = defaultdict(
    lambda: {'up': set(), 'primary': set(), 'acting': set()}
)

# map pg -> osds involved
pg_osds_up = defaultdict(set)
pg_osds_acting = defaultdict(set)

# pg metadata
# pgid -> pg dump pgstats entry
pgs = dict()

for pg in pg_dump["pg_map"]["pg_stats"]:
    pgid = pg["pgid"]
    up = pg["up"]
    acting = pg["acting"]
    primary = acting[0]

    pg_osds_up[pgid] = up
    pg_osds_acting[pgid] = acting

    osd_mappings[primary]['primary'].add(pgid)

    for osd in up:
        osd_mappings[osd]['up'].add(pgid)
    for osd in acting:
        osd_mappings[osd]['acting'].add(pgid)

    pgs[pgid] = pg


osds = dict()

# gather which pgs are on what osd
for osdid, osd in osd_mappings.items():
    osd_pools_up = set()
    osd_pools_acting = set()

    pgs_up = set()
    pgs_acting = set()

    pg_count_up = defaultdict(int)
    pg_count_acting = defaultdict(int)

    for pg in osd['up']:
        poolid = int(pg.split('.', maxsplit=1)[0])
        osd_pools_up.add(poolid)
        pgs_up.add(pg)

        pg_count_up[poolid] += 1

    for pg in osd['acting']:
        poolid = int(pg.split('.', maxsplit=1)[0])
        osd_pools_acting.add(poolid)
        pgs_acting.add(pg)

        pg_count_acting[poolid] += 1


    if osdid == 0x7fffffff:
        osdid = -1
        crushclass = "-"
    else:
        crushclass = crushclasses[osdid]

    osds[osdid] = {
        'pools_up': list(sorted(osd_pools_up)),
        'pools_acting': list(sorted(osd_pools_acting)),
        'pg_count_up': pg_count_up,
        'pg_count_acting': pg_count_acting,
        'pg_num_up': len(pgs_up),
        'pgs_up': pgs_up,
        'pg_num_acting': len(pgs_acting),
        'pgs_acting': pgs_acting,
        'crush_class': crushclass,
        'utilization': -1,
        "device_size": -1,
        "device_used": -1,
        "crush_weight": -1,
    }


for osd in osd_df_dump["nodes"]:
    id = osd["id"]
    osds[id].update({
        "device_size": osd["kb"] * 1024,
        "device_used": osd["kb_used"] * 1024,
        "device_used_data": osd["kb_used_data"] * 1024,
        "device_used_meta": osd["kb_used_meta"] * 1024,
        "utilization": osd["utilization"],
        "crush_weight": osd["crush_weight"],
    })

    if osd['pgs'] != osds[id]['pg_num_acting']:
        raise Exception(f"on osd.{id} calculated pg num acting: "
                        f"{osds[id]['pg_num_acting']} != {osd['pgs']}")

for osd in osd_dump["osds"]:
    id = osd["osd"]
    osds[id].update({
        "weight": osd["weight"],
        "cluster_addr": osd["cluster_addr"],
        "public_addr": osd["public_addr"],
        "state": tuple(osd["state"]),
    })


# create the crush trees
buckets = crush_dump["buckets"]

# bucketid -> bucket dict
bucket_ids_tmp = dict()

# all bucket ids of roots
bucket_root_ids = list()

for device in crush_dump["devices"]:
    id = device["id"]
    assert id >= 0
    bucket_ids_tmp[id] = device

for bucket in buckets:
    id = bucket["id"]
    assert id < 0
    bucket_ids_tmp[id] = bucket

    if bucket["type_name"] == "root":
        bucket_root_ids.append(id)


def bucket_fill(id, parent_id=None):
    """
    returns the list of all child buckets for a given id
    plus for each of those, their children.
    """
    bucket = bucket_ids_tmp[id]

    children = list()
    ids = dict()

    this_bucket = {
        "id": id,
        "name": bucket["name"],
        "type_name": bucket["type_name"],
        "weight": bucket["weight"],
        "parent": parent_id,
        "children": children,
    }
    ids[id] = this_bucket

    for child_item in bucket["items"]:
        child = bucket_ids_tmp[child_item["id"]]
        cid = child["id"]
        if cid < 0:
            new_nodes, new_ids = bucket_fill(cid, id)
            ids.update(new_ids)
            children.extend(new_nodes)

        else:
            # it's a device
            new_node = {
                "id": cid,
                "name": child["name"],
                "type_name": "osd",
                "class": child["class"],
                "parent": id,
            }
            ids[cid] = new_node
            children.append(new_node)

    return this_bucket, ids

# populare all roots
bucket_roots = list()
for root_bucket_id in bucket_root_ids:
    bucket_tree, bucket_ids = bucket_fill(root_bucket_id)
    bucket_roots.append((bucket_tree, bucket_ids))

del bucket_ids_tmp


@lru_cache(maxsize=2048)
def trace_crush_root(osd, root):
    """
    in the given root, trace back all items from the osd up to the root
    """
    found = False
    for root_bucket, try_root_ids in bucket_roots:
        if root_bucket["name"] == root:
            root_ids = try_root_ids
            break

    if not root_ids:
        raise Exception(f"crush root {root} not known?")

    try_node_in_root = root_ids.get(osd)
    if try_node_in_root is None:
        # osd is not part of this root, i.e. wrong device class
        return None

    node_id = try_node_in_root["id"]
    assert node_id == osd

    # walk from leaf (osd) to the tree root
    bottomup = list()
    while True:
        if node_id is None:
            # we reached the root
            break

        bottomup.append({
            "id": node_id,
            "type_name": root_ids[node_id]["type_name"],
        })

        if root_ids[node_id]["name"] == root:
            found = True
            break

        node_id = root_ids[node_id]["parent"]

    if not found:
        raise Exception(f"could not find a crush-path from osd={osd} to {root!r}")

    topdown = list(reversed(bottomup))
    return topdown


def pool_from_pg(pg):
    return int(pg.split(".")[0])


@lru_cache(maxsize=2**20)
def get_pg_shardsize(pgid):
    pg_stats = pgs[pgid]['stat_sum']
    shard_size = pg_stats['num_bytes']
    shard_size += pg_stats['num_omap_bytes']

    pool_id = pool_from_pg(pgid)
    pool = pools[pool_id]
    ec_profile = pool["erasure_code_profile"]
    if ec_profile:
        shard_size /= ec_profiles[ec_profile]["data_chunks"]
        # omap is not supported on EC pools (yet)
        # when it is, check how the omap data is spread (replica or also ec?)

    return shard_size


def find_item_type(trace, item_type, rule_depth, item_uses):
    for idx, item in enumerate(trace, start=1):
        if item["type_name"] == item_type:
            item_uses[rule_depth][item["id"]] += 1
            return idx
    return None


def rule_for_pg(pg):
    """
    get the crush rule for a pg.
    """
    pool = pools[pool_from_pg(move_pg)]
    crushruleid = pool['crush_rule']
    return crushrules[crushruleid]


def rootname_from_rule(rule):
    """
    return the crush root name for the given rule
    """
    root_name = None
    for step in rule["steps"]:
        if step["op"] == "take":
            root_name = step["item_name"]
            break

    if not root_name:
        raise Exception(f"rule has no take step")

    return root_name


def candidates_for_root(root_name):
    """
    get the set of all osds where a crush rule could place shards.
    """

    for root_bucket, try_root_ids in bucket_roots:
        if root_bucket["name"] == root_name:
            root_ids = try_root_ids
            break

    if not root_ids:
        raise Exception(f"crush root {root} not known?")

    return {nodeid for nodeid in root_ids.keys() if nodeid >= 0}


class PGMoveChecker:
    """
    for the given rule and utilized pg_osds,
    create a checker that can verify osd replacements are valid.
    """

    def __init__(self, pg_mappings, move_pg):
        # which pg to relocate
        self.pg = move_pg

        self.pool = pools[pool_from_pg(move_pg)]
        self.pool_size = self.pool["size"]
        self.rule = crushrules[self.pool['crush_rule']]

        # crush root name for the pg
        self.root_name = rootname_from_rule(self.rule)

        # all available placement osds for this crush root
        self.osd_candidates = candidates_for_root(self.root_name)

        self.pg_mappings = pg_mappings  # current pg->[osd] mapping state
        self.pg_osds = pg_mappings.get_mapping(move_pg)  # acting osds managing this pg

    def get_osd_candidates(self):
        """
        return all possible candidate OSDs for the PG to relocate.
        """
        return self.osd_candidates

    def prepare_crush_check(self):
        """
        perform precalculations for moving this pg
        """

        # ruledepth -> allowed number of bucket reuse
        reuses_per_step = []
        fanout_cum = 1

        # calculate how often one bucket layer can be reused
        # this is the crush-constraint, set up by the rule
        for step in reversed(self.rule["steps"]):
            if step["op"] == "take":
                num = 1
            elif step["op"].startswith("choose"):
                num = step["num"]
            elif step["op"] == "emit":
                num = 1
            else:
                continue

            reuses_per_step.append(fanout_cum)

            if num <= 0:
                num += self.pool_size

            fanout_cum *= num
        reuses_per_step.reverse()

        # for each depth, count how often items were used
        # rule_depth -> {itemid -> use count}
        item_uses = defaultdict(lambda: defaultdict(lambda: 0))

        # example: 2+2 ec -> size=4
        #
        # root        __________-9______________________________
        # rack: _____-7_______    _________-8_____        ___-10____
        # host: -1    -2    -3    -4    -5      -6        -11     -12
        # osd: 1 2 | 3 4 | 5 6 | 7 8 | 9 10 | 11 12 |   13 14 | 15 16
        #        _     _         _     _
        #
        # take root
        # choose 2 racks
        # chooseleaf 2 hosts
        #
        # fanout: rule step's num = selections below bucket
        # [1, 2, 2]
        #
        # inverse aggregation, starting with 1
        # reuses_per_step = [4, 2, 1]
        #
        # current pg=[2, 4, 7, 9]
        #
        # Now: replace_osd 2
        #
        # traces: x 2: [-9, -7, -1, 2]
        #           4: [-9, -7, -2, 4]
        #           7: [-9, -8, -4, 7]
        #           9: [-9, -8, -5, 9]
        #
        #
        # use counts - per rule depth.
        #  {0: {-9: 4}, 1: {-7: 2, -8: 2}, 2: {-1: 1, -2: 1, -4: 1, -5: 1}}
        #
        # from this use count, subtract the trace of the replaced osd
        #
        # now eliminate candidates:
        # * GET TRACE FROM THEM
        # * check use counts against reuses_per_step
        #
        # 1 -> -9 used 3<4, -7 used 1<2, -1 used 0<1 -> ok
        # 2 -> replaced..
        # 3 -> -9 used 3<4, -7 used 1<2, -2 used 1<1 -> fail
        # 4 -> keep, not replaced
        # 5 -> -9 used 3<4, -7 used 1<2, -3 used 0<1 -> ok
        # 6 -> -9 used 3<4, -7 used 1<2, -3 used 0<1 -> ok
        # 7 -> keep, not replaced
        # 8 -> -9 used 3<4, -8 used 2<2, -4 used 1<1 -> fail
        # ...
        #
        # replacement candidates for 2: 1, 5, 6, 13 14 15 16
        #

        # collect trace for each osd
        tree_depth = 0
        rule_depth = 0  # because we skip steps like chooseleaf_tries
        rule_root_name = None

        # osd -> crush-root-trace
        constraining_traces = dict()
        emit = False

        # rule_depth -> tree_depth to next rule (what tree layer is this rule step)
        # because "choose" steps may skip layers in the crush hierarchy
        rule_tree_depth = dict()

        # gather item usages by evaluating the crush rules
        for step in self.rule["steps"]:
            if step["op"] == "take":
                rule_root_name = step["item_name"]

                # should be the same since we fetch it the exact same way
                assert rule_root_name == self.root_name

                # first step: try to find tracebacks for all osds that ends up in this root.
                constraining_traces = dict()

                for pg_osd in self.pg_osds:
                    trace = trace_crush_root(pg_osd, rule_root_name)
                    if trace is None:
                        raise Exception(f"no trace found for {pg_osd} in {rule_root_name}")
                    constraining_traces[pg_osd] = trace
                    # the root was "used"
                    item_uses[rule_depth][trace[0]["id"]] += 1

                rule_tree_depth[rule_depth] = 0
                tree_depth = 1
                rule_depth += 1

            elif step["op"].startswith("choose"):
                if not constraining_traces:
                    raise Exception('no backtraces captured from rule (missing "take"?)')

                choose_type = step["type"]

                # find the new tree_depth by looking for the choosen next bucket
                # increase item counter for current osd't trace

                for constraining_trace in constraining_traces.values():
                    steps_taken = find_item_type(constraining_trace[tree_depth:], choose_type, rule_depth, item_uses)
                    if steps_taken is None:
                        raise Exception(f"could not find item type {step['type']} "
                                        f"requested by rule step {step}")

                # how many layers we went down the tree
                rule_tree_depth[rule_depth] = tree_depth + steps_taken
                tree_depth += steps_taken
                rule_depth += 1

            elif step["op"] == "emit":
                emit = True

                type_found = False
                for constraining_trace in constraining_traces.values():
                    steps_taken = find_item_type(constraining_trace[tree_depth:], "osd", rule_depth, item_uses)
                    if steps_taken is None:
                        raise Exception(f"could not find item type {step['type']} "
                                        f"requested by rule step {step}")

                rule_tree_depth[rule_depth] = tree_depth + steps_taken
                tree_depth += steps_taken
                rule_depth += 1

                # sanity checks lol
                assert len(rule_tree_depth) == rule_depth
                assert len(item_uses) == rule_depth
                for idx, reuses in enumerate(reuses_per_step):
                    for item, uses in item_uses[idx].items():
                        assert reuses == uses

                # only one emit supported so far
                break

            else:
                pass

        if not emit:
            raise Exception("uuh no emit seen?")

        if not constraining_traces:
            raise Exception("no tree traces gathered?")

        # validate item uses:
        for i in range(rule_depth):
            for item, uses in item_uses[i].items():
                if uses != reuses_per_step[i]:
                    print(f"reuses: {reuses_per_step}")
                    print(f"item_uses: {pformat(item_uses)}")
                    raise Exception("counted item uses != crush item, in step {i}: "
                                    f"{item}={uses} != {reuses_per_step[i]}")

        # should still be the same, it could have changed if there were
        # multiple takes
        assert rule_root_name == self.root_name

        self.constraining_traces = constraining_traces  # osdid->crush-root-trace
        self.rule_tree_depth = rule_tree_depth  # rulestepid->tree_depth
        self.reuses_per_step = reuses_per_step  # rulestepid->allowed_item_reuses
        self.item_uses = item_uses  # rulestepid->{item->use_count}

    def is_move_valid(self, old_osd, new_osd):
        """
        verify that the given new osd does not violate the
        crush rules' constraints of placement.
        """
        if new_osd in self.pg_osds:
            logging.debug(f"   invalid: {new_osd} in {self.pg_osds}")
            return False

        if new_osd not in self.osd_candidates:
            logging.debug(f"   invalid: {new_osd} not in same crush root")
            return False

        # create trace for the replacement candidate
        new_trace = trace_crush_root(new_osd, self.root_name)

        if new_trace is None:
            # probably not a compatible device class
            logging.debug(f"   no trace found for {new_osd}")
            return False

        # the trace we no longer consider (since we replace the osd)
        old_trace = self.constraining_traces[old_osd]

        overuse = False
        for idx, tree_stepwidth in enumerate(self.rule_tree_depth):
            use_max_allowed = self.reuses_per_step[idx]

            # as we would remove the old osd trace,
            # the item would no longer be occupied in the new trace
            old_item = old_trace[tree_stepwidth]["id"]
            # this trace now adds to the item uses:
            new_item = new_trace[tree_stepwidth]["id"]

            # how often is new_item used now?
            # if not used at all, it's not in the dict.
            uses = self.item_uses[idx].get(new_item, 0)

            if old_item == new_item:
                uses -= 1

            # if we used it, it'd be violating crush
            # (the +1 was 'optimized' by >= instead of >)
            if uses >= use_max_allowed:
                logging.debug(f"   invalid: {new_osd} violates crush: using {new_item} x {uses+1} > {use_max_allowed}")
                overuse = True
                break

        return not overuse

    def get_placement_variance(self, osd_from=None, osd_to=None):
        """
        calculate the variance of weighted OSD usage
        for all OSDs that are candidates for this PG

        osd_from -> osd_to: how would the variance look, if
                            we had moved data.
        """

        osds_used = list()
        for osd in self.osd_candidates:
            remapping = None
            if osd_from is not None and osd_to is not None:
                remapping = {self.pg: [(osd_from, osd_to)]}
            elif osd_from or osd_to:
                raise Exception("placement variance with remapping "
                                "requires both from and to osds")
            osd_used = self.pg_mappings.get_osd_usage(osd, remapping)

            osds_used.append(osd_used)

        var = statistics.variance(osds_used)
        return var

    def filter_candidates(self, osdids):
        """
        given an iterator of osd ids, return only those
        entries that are in the same crush root
        """
        for osdid in osdids:
            if osdid not in self.osd_candidates:
                continue
            yield osdid


class PGMappings:
    """
    PG mapping simulator
    used to calculate device usage when moving around pgs.
    """
    def __init__(self, pgs, osds):

        # the "real" devices, just used for their "fixed" properties like
        # device size
        self.osds = osds

        # osdid -> {pg, ...}
        self.osd_pgs = defaultdict(set)

        # pgid -> [up_osd, ...]
        self.pg_mappings = dict()
        for pg, pginfo in pgs.items():
            # choose the up mapping, since we wanna optimize the "future" cluster
            up_osds = pginfo["up"]
            self.pg_mappings[pg] = list(up_osds)

            for osd in up_osds:
                self.osd_pgs[osd].add(pg)

        # pg->[(osd_from, osd_to), ...]
        self.remaps = defaultdict(list)

    def apply_remap(self, pg, osd_from, osd_to):
        """
        simulate a remap pg from one osd to another.
        this updates the mappings stored in this object.
        """

        self.osd_pgs[osd_from].remove(pg)
        self.osd_pgs[osd_to].add(pg)

        pg_mapping = self.pg_mappings[pg]
        did_remap = False
        for i in range(len(pg_mapping)):
            if pg_mapping[i] == osd_from:
                logging.debug(f"recording move of pg={pg} from {osd_from}->{osd_to}")
                pg_mapping[i] = osd_to
                did_remap = True
                break

        if not did_remap:
            raise Exception(f"did not find osd {osd_from} in pg {pg} mapping")

        self.remaps[pg].append((osd_from, osd_to))

    def get_mapping(self, pg):
        return self.pg_mappings[pg]

    def get_osd_pgs(self, osd):
        return self.osd_pgs[osd]

    def get_osd_occupied(self, osd):
        """
        calculate the osd usage by summing all the mapped PGs shardsizes
        -> we can calculate a future size
        """
        used = 0

        for pg in self.get_osd_pgs(osd):
            used += get_pg_shardsize(pg)

        return used

    def get_osd_weighted_size(self, osdid):
        """
        return the weighted OSD device size
        """

        osd = self.osds[osdid]
        size = osd['device_size']
        weight = osd['weight']

        return size * weight

    def get_osd_usage(self, osdid, remap_pg=None, add_size=0):
        """
        returnes the occupied OSD space, weighted by device size and device weight.

        to calculate a simulated, different size, pass remap_pg.
        remap_pg: {'pgid': [(osd_from, osd_to), ...], ...}

        another variant is add to the used data amount with add_size.
        """

        osd_size = self.get_osd_weighted_size(osdid)
        if osd_size == 0:
            raise Exception("getting relative usage of a 0-weighted osd impossible")

        used = self.get_osd_occupied(osdid)

        use_delta = 0

        if remap_pg:
            for pgid, remappings in remap_pg.items():
                shard_size = get_pg_shardsize(pgid)
                for remap_from, remap_to in remappings:
                    if osdid == remap_from:
                        use_delta = -shard_size
                    elif osdid == remap_to:
                        use_delta = shard_size

        used += use_delta
        used += add_size

        # make it relative
        used /= osd_size
        used *= 100

        return used

    def get_upmaps(self):
        """
        get all applied mappings
        return {pgid: [(map_from, map_to), ...]}
        """
        # pgid -> [(map_from, map_to), ...]
        upmap_results = dict()

        for pg, remaps_list in self.remaps.items():

            # remaps is [(osdfrom, osdto), ...], now osdfrom->osdto
            # these are the "new" remappings.
            remaps = dict(remaps_list)

            # merge new upmaps
            # i.e. (1, 2), (2, 3) => (1, 3)
            for new_from, new_to in remaps_list:
                other_to = remaps.get(new_to)
                if other_to is not None:
                    remaps[new_from] = other_to
                    del remaps[new_to]

            # current_upmaps are is [(osdfrom, osdto), ...]
            current_upmaps = upmap_items.get(pg, [])

            # now, let's merge current_upmaps and remaps:

            # which of the current_upmaps to retain
            resulting_upmaps = list()

            # what remap-source-osds we already covered with merges
            merged_remaps = set()

            for current_from, current_to in current_upmaps:
                # is the previous to-device one we would map to somewhere else
                remapped_to = remaps.get(current_to)
                if remapped_to:
                    # this remap's source osd will now be merged
                    merged_remaps.add(current_to)

                    # transform e.g. current=197->188, remapped=188->261 to 197->261
                    resulting_upmaps.append((current_from, remapped_to))
                    continue

                resulting_upmaps.append((current_from, current_to))

            for new_from, new_to in remaps.items():
                if new_from in merged_remaps:
                    continue
                resulting_upmaps.append((new_from, new_to))

            upmap_results[pg] = resulting_upmaps

        return upmap_results

    def osd_pool_pg_count(self, osd):
        """
        return {pool -> pg_count} for an OSD
        """
        ret = defaultdict(lambda: 0)
        for pg in self.get_osd_pgs(osd):
            ret[pool_from_pg(pg)] += 1

        return ret

    def pool_pg_count_ideal(self, poolid, candidate_osds):
        """
        return the ideal pg count for a poolid,
        given the candidate osd ids.
        expressed in the unit of osd weight, so probably TiB
        """

        pool = pools[poolid]
        pool_total_pg_count = pool['size'] * pool['pg_num']

        size_sum = 0

        for osdid in candidate_osds:
            size_sum += self.get_osd_weighted_size(osdid)

        # uuh somehow no osd had a size or all weights 0?
        assert size_sum > 0

        pgs_per_size = pool_total_pg_count / size_sum

        return pgs_per_size

    def osd_pool_pg_count_ideal(self, poolid, osdid, candidate_osds):
        """
        return the ideal pg count for a pool id for some osdid.
        """

        osd_size = self.get_osd_weighted_size(osdid)
        if osd_size == 0:
            return 0

        ideal_relative_count = self.pool_pg_count_ideal(poolid, candidate_osds)
        return ideal_relative_count * osd_size


def A001057():
    """
    generate [0, 1, -1, 2, -2, 3, -3, ...]
    https://oeis.org/A001057
    """
    idx = 0
    while True:
        val = int((1 - (2 * idx + 1) * (-1)**idx) / 4)
        idx += 1
        yield val


if args.mode == 'balance':
    logging.info("running pg balancer")

    # this is basically my approach to OSDMap::calc_pg_upmaps
    # and a CrushWrapper::try_remap_rule python-implementation

    only_poolids = None
    if args.only_poolid:
        only_poolids = {int(pool) for pool in args.only_poolid.split(",") if pool.strip()}
        logging.info(f"only considering pools {only_poolids}")

    if args.only_crushclass:
        only_crushclasses = {cls.strip() for cls in args.only_crushclass.split(",") if cls.strip()}
        logging.info(f"only considering crushclasses {only_crushclasses}")

        osd_candidates = {osd for osd in osds.keys() if crushclasses.get(osds) in only_crushclasses}
    else:
        osd_candidates = osds.keys()

    # filter osd candidates by weight, and remove weight=0
    osd_candidates = {osd for osd in osd_candidates if osds[osd]['weight'] != 0}

    # we'll do all the optimizations in this mapping state
    pg_mappings = PGMappings(pgs, osds)

    found_remap_count = 0

    while True:

        if found_remap_count >= args.max_pg_moves:
            logging.info("enough remaps found")

            for pg, upmaps in pg_mappings.get_upmaps().items():
                upmap_str = " ".join([f"{osd_from} {osd_to}" for (osd_from, osd_to) in upmaps])
                print(f"ceph osd pg-upmap-items {pg} {upmap_str}")
            break

        # osd -> usage
        # if an osd has a weight of 0, it's 100% full.
        osd_usages = {osd: pg_mappings.get_osd_usage(osd) for osd in osd_candidates}

        # from this mapping, calculate the weighted osd size,
        # and start by taking the fullest OSD
        osds_usage_asc = list(sorted(osd_usages.items(), key=lambda osd: osd[1]))

        found_remap = False

        unsuccessful_pools = set()

        # try to move the biggest pg from the fullest disk to the next suiting smaller disk
        for osd_from, osd_from_used_percent in reversed(osds_usage_asc):
            if found_remap:
                break

            logging.debug("trying to empty osd.%s (%f %%)", osd_from, osd_from_used_percent)

            # pg -> shardsize
            pg_candidates_sizes = dict()
            for pg_candidate in pg_mappings.get_osd_pgs(osd_from):
                pg_pool = pool_from_pg(pg_candidate)
                if only_poolids and pg_pool not in only_poolids:
                    continue

                pg_candidate_size = get_pg_shardsize(pg_candidate)
                pg_candidates_sizes[pg_candidate] = pg_candidate_size

            # remaining pgs sorted by their shardsize, descending
            pg_candidates_desc = list(sorted(pg_candidates_sizes.items(),
                                             key=lambda pg: pg[1], reverse=True))
            pg_candidates_desc_sizes = [pg[1] for pg in pg_candidates_desc]

            # here, we decide to move not the largest/smallest pg, but rather the median one.
            # order PGs around the median-sized one
            # [5, 4, 3, 2, 1] => [3, 4, 2, 5, 1]
            pg_candidates = list()
            pg_candidates_median = statistics.median_low(pg_candidates_desc_sizes)
            pg_walk_anchor = pg_candidates_desc_sizes.index(pg_candidates_median)
            for walk_jump in A001057():
                pg_walk_pos = pg_walk_anchor + walk_jump

                if pg_walk_pos < -1:
                    break
                elif pg_walk_pos >= len(pg_candidates_desc) + 1:
                    break
                elif pg_walk_pos < 0:
                    continue
                elif pg_walk_pos >= len(pg_candidates_desc):
                    continue

                pg = pg_candidates_desc[pg_walk_pos]
                pg_candidates.append(pg)

            assert len(pg_candidates) == len(pg_candidates_desc)

            from_osd_pg_count = pg_mappings.osd_pool_pg_count(osd_from)

            # now try to move the candidates to their possible destination
            for move_pg, move_pg_shardsize in pg_candidates:
                if found_remap:
                    break

                pg_pool = pool_from_pg(move_pg)

                if pg_pool in unsuccessful_pools:
                    logging.debug("SKIP pg %s since pool (%s) can't be balanced more", move_pg, pg_pool)
                    continue

                logging.debug("TRY-0 moving pg %s with %s from osd.%s", move_pg, pprintsize(move_pg_shardsize), osd_from)

                try_pg_move = PGMoveChecker(pg_mappings, move_pg)
                pool_pg_count_ideal = pg_mappings.pool_pg_count_ideal(pg_pool, try_pg_move.get_osd_candidates())
                from_osd_pg_count_ideal = pool_pg_count_ideal * pg_mappings.get_osd_weighted_size(osd_from)

                # only move the pg if the source osd has more PGs of the pool than average
                # otherwise the regular balancer will fill this OSD again
                # with another PG (of the same pool) from somewhere
                if from_osd_pg_count[pg_pool] <= from_osd_pg_count_ideal:
                    logging.debug("  BAD => skipping pg %s since source osd.%s "
                                  "doesn't have too many of pool=%s (%s <= %s)",
                                  move_pg, osd_from, pg_pool, from_osd_pg_count[pg_pool], from_osd_pg_count_ideal)
                    continue
                logging.debug("  OK => taking pg %s from source osd.%s "
                              "since it has too many of pool=%s (%s > %s)",
                              move_pg, osd_from, pg_pool, from_osd_pg_count[pg_pool], from_osd_pg_count_ideal)

                pg_small_enough = False
                for osd_to in try_pg_move.get_osd_candidates():
                    # check if there's a target osd that will be filled less than the source osd after move
                    # TODO: gradually increase limit for pg size so that:
                    #       predicted_target_usage < ((target_usage + source_usage)/2 + limit
                    target_predicted_usage = pg_mappings.get_osd_usage(osd_to, add_size=move_pg_shardsize)
                    if target_predicted_usage < osd_from_used_percent:
                        pg_small_enough = True
                        break

                if not pg_small_enough:
                    # the pg is so big, it would increase the fill % of all osd_tos more than osd_from is
                    # so we would increase the variance.
                    logging.debug("  BAD => skipping pg %s, since its not small enough", move_pg)
                    continue

                # TODO: start with the smallest PG and check if its movement meets newusage < (oldusage + sourceusage)/2
                # increase pg size until this is no longer met, and move the one that fitted best

                try_pg_move.prepare_crush_check()

                # check variance for this crush root
                variance_before = try_pg_move.get_placement_variance()

                # try the least full osd that's allowed by crush
                for osd_to in try_pg_move.filter_candidates(osd[0] for osd in osds_usage_asc):
                    logging.debug(f"TRY-1 move {move_pg} osd.{osd_from} => osd.{osd_to}")

                    # in order to not fight the regular balancer, don't move the PG to a disk
                    # where the weighted pg count of this pool is already good
                    # otherwise the balancer will move a pg on the osd_from of this pool somewhere else
                    # i.e. don't be the +1
                    to_osd_pg_count = pg_mappings.osd_pool_pg_count(osd_to)
                    to_osd_pg_count_ideal = pool_pg_count_ideal * pg_mappings.get_osd_weighted_size(osd_to)
                    if to_osd_pg_count[pg_pool] >= to_osd_pg_count_ideal:
                        logging.debug(f"   BAD => osd.{osd_to} already has too many of pool={pg_pool} "
                                      f"({to_osd_pg_count[pg_pool]} >= {to_osd_pg_count_ideal})")
                        continue
                    logging.debug(f"   OK => osd.{osd_to} has too few of pool={pg_pool} "
                                  f"({to_osd_pg_count[pg_pool]} < {to_osd_pg_count_ideal})")

                    if not try_pg_move.is_move_valid(osd_from, osd_to):
                        logging.debug(f"   BAD move {move_pg} osd.{osd_from} => osd.{osd_to}")
                        continue

                    # check if the variance is decreasing
                    new_variance = try_pg_move.get_placement_variance(osd_from, osd_to)

                    if new_variance >= variance_before:
                        logging.debug(f"   BAD => variance not decreasing: {new_variance} not < {variance_before}")
                        continue

                    new_pg_mapping = list()
                    for osdid in pg_osds_up[move_pg]:
                        if osdid == osd_from:
                            osdid = osd_to
                        new_pg_mapping.append(osdid)

                    logging.info(f"   GOOD move {move_pg} osd.{osd_from} => osd.{osd_to} "
                                 f"(size={pprintsize(move_pg_shardsize)})")
                    logging.info(f"    pg {move_pg} is then on {new_pg_mapping}")
                    logging.info(f"    => variance decreasing: {new_variance} < {variance_before}")

                    pg_mappings.apply_remap(move_pg, osd_from, osd_to)

                    found_remap = True
                    found_remap_count += 1
                    break

                if not found_remap:
                    # we tried all osds to place this pg,
                    # so the shardsize is just too big
                    # TODO: we can likely check this earlier and more efficient..
                    unsuccessful_pools.add(pg_pool)


elif args.mode == 'show':
    if args.format == 'plain':
        poolname = 'name'.rjust(maxpoolnamelen)
        print()
        print(f"{'poolid': >6} {poolname} {'type': >7} {'size': >5} {'min': >3} {'pg_num': >6} {'stored': >7} {'used': >7} {'avail': >7} {'shrdsize': >8} crush")

        # default, sort by pool id
        sort_func = lambda x: x[0]

        if args.sort_shardsize:
            sort_func = lambda x: x[1]['pg_shard_size_avg']

        for poolid, poolprops in sorted(pools.items(), key=sort_func):
            poolname = poolprops['name'].rjust(maxpoolnamelen)
            repl_type = poolprops['repl_type']
            if repl_type == "ec":
                profile = ec_profiles[poolprops['erasure_code_profile']]
                repl_type = f"ec{profile['data_chunks']}+{profile['coding_chunks']}"
            crushruleid = poolprops['crush_rule']
            size = poolprops['size']
            min_size = poolprops['min_size']
            pg_num = poolprops['pg_num']
            stored = pprintsize(poolprops['stored'])  # used data excl metadata
            used = pprintsize(poolprops['used'])  # raw usage incl metastuff
            avail = pprintsize(poolprops['store_avail'])
            shard_size = pprintsize(poolprops['pg_shard_size_avg'])
            print(f"{poolid: >6} {poolname} {repl_type: >7} {size: >5} {min_size: >3} {pg_num: >6} {stored: >7} {used: >7} {avail: >7} {shard_size: >8} {crushruleid}:{crushrules[crushruleid]['name']}")

        if args.osds:
            print()
            if maxcrushclasslen < len('class'):
                maxcrushclasslen = len('class')
            crushclass = 'class'.rjust(maxcrushclasslen)

            osd_entries = list()

            for osdid, props in osds.items():
                weight_val = props['weight']

                if args.use_shardsize_sum:
                    used = 0
                    if args.pgstate == 'up':
                        placed_pgs = props['pgs_up']
                    elif args.pgstate == 'acting':
                        placed_pgs = props['pgs_acting']
                    else:
                        raise Exception("unknown pgstate")

                    for pg in placed_pgs:
                        used += get_pg_shardsize(pg)

                    util_val = 100 * used / props['device_size']

                else:
                    util_val = 100 * props['device_used'] / props['device_size']

                if args.use_weighted_utilization:
                    if weight_val == 0:
                        util_val = 0
                    else:
                        util_val /= weight_val

                if util_val < args.osd_fill_min:
                    continue

                util = "%.1f%%" % util_val
                weight = "%.3f" % weight_val
                crushclass = props['crush_class'].rjust(maxcrushclasslen)
                devsize = props['device_size'] * props['weight']

                if args.pgstate == 'up':
                    pg_count = props['pg_count_up']
                    pg_num = props['pg_num_up']
                elif args.pgstate == 'acting':
                    pg_count = props['pg_count_acting']
                    pg_num = props['pg_num_acting']
                else:
                    raise Exception("unknown pgstate")

                pool_list = dict()
                for pool, count in sorted(pg_count.items()):

                    if args.normalize_pg_count:
                        # normalize to terrabytes
                        if devsize >= 0:
                            count /= devsize / 1024 ** 4
                        else:
                            count = 0

                    pool_list[pool] = count

                osd_entries.append((osdid, crushclass, devsize, weight, util, pg_num, pool_list))

            # default sort by osdid
            sort_func = lambda x: x[0]

            if args.sort_utilization:
                sort_func = lambda x: x[4]

            if args.sort_pg_count is not None:
                sort_func = lambda x: x[5].get(args.sort_pg_count, 0)

            # header:
            print(f"{'osdid': >6} {crushclass}  {'devsize': >7}  {'weight': >6}  {'util': >5} {'pg_num': >6}  pools")
            for osdid, crushclass, devsize, weight, util, pg_num, pool_pgs in sorted(osd_entries, key=sort_func):

                pool_overview = list()
                for pool, count in pool_pgs.items():
                    if args.per_pool_count:
                        if type(count) == float:
                            entry = f"{pool}({count:.1f})"
                        else:
                            entry = f"{pool}({count})"
                    else:
                        entry = f"{pool}"

                    if args.sort_pg_count == pool:
                        pool_overview.insert(0, entry)
                    else:
                        pool_overview.append(entry)

                pool_list_str = ' '.join(pool_overview)
                print(f"{osdid: >6} {crushclass}  {pprintsize(devsize): >7}  {weight: >6}  {util: >5} {pg_num: >6}  {pool_list_str}")

    elif args.format == 'json':
        ret = {
            'pgstate': args.pgstate,
            'pools': pools,
            'osds': osds,
        }

        print(json.dumps(ret))

elif args.mode == 'showremapped':

    for pg, pginfo in pgs.items():
        pgstate = pginfo["state"].split("+")
        if "remapped" in pgstate:
            up_osds = pginfo["up"]
            acting_osds = pginfo["acting"]

            moves = list()
            for up_osd, acting_osd in zip(up_osds, acting_osds):
                if up_osd != acting_osd:
                    moves.append(f"{acting_osd}->{up_osd}")

            objs_total = pginfo["stat_sum"]["num_objects"]
            objs_misplaced = pginfo["stat_sum"]["num_objects_misplaced"]
            if objs_total > 0:
                progress = 1 - (objs_misplaced / (objs_total * len(moves)))
            else:
                progress = 1
            progress *= 100

            state = "backfill" if "backfilling" in pgstate else "waiting "
            move_size = pprintsize(get_pg_shardsize(pg))
            print(f"pg {pg} {state} {move_size: >5}: {objs_misplaced} of {objs_total}, {progress:.1f}%, {','.join(moves)}")


else:
    raise Exception(f"unknown args mode {args.mode}")
