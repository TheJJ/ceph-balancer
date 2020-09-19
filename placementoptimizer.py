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

# even "better" algorithm:
# get osdmap and crushmap
# calculate constraints weighted by device
# get current utilization weighted by device
# create z3 equation using these constraints
# transform result to upmap items


import argparse
import subprocess
import json
import shlex
import statistics
from collections import defaultdict
from functools import lru_cache
from pprint import pformat


cli = argparse.ArgumentParser()

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
                    help="which PG state to consider: up or acting. default: %(default)s")
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
showsp.add_argument('--osd-fill-min', type=int, default=0,
                    help='minimum fill %% to show an osd, default: %(default)s%%')

sp.add_parser('showremapped')

balancep = sp.add_parser('balance')
balancep.add_argument('--only-poolid',
                      help='comma separated list of pool ids to consider for balancing')
balancep.add_argument('--only-crushclass',
                      help='comma separated list of crush classes to balance')


args = cli.parse_args()


def jsoncall(cmd):
    if not isinstance(cmd, list):
        raise ValueError("need cmd as list")

    rawdata = subprocess.check_output(cmd)
    return json.loads(rawdata.decode())


def pprintsize(size_bytes):
    prefixes = ((1, 'K'), (2, 'M'), (3, 'G'), (4, 'T'), (5, 'E'), (6, 'Z'))
    for exp, name in prefixes:
        if size_bytes >= 1024 ** exp and size_bytes < 1024 ** (exp + 1):
            new_size = size_bytes / 1024 ** exp
            return "%.1f%s" % (new_size, name)

    return "%.1fB" % size_bytes


# this is shitty: this whole script depends on these outputs,
# but they might be inconsistent, if the cluster had changes
# between calls....
pg_dump = jsoncall(["ceph", "pg", "dump", "--format", "json"])
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
        'pg_num': pool["pg_num"],
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


def pg_shardsize(pg):
    shard_size = pgs[pg]['stat_sum']['num_bytes']
    pool_id = pool_from_pg(pg)
    pool = pools[pool_id]
    ec_profile = pool["erasure_code_profile"]
    if ec_profile:
        shard_size /= ec_profiles[ec_profile]["data_chunks"]

    return shard_size


def find_item_type(trace, item_type, rule_depth, item_uses):
    for idx, item in enumerate(trace, start=1):
        if item["type_name"] == item_type:
            item_uses[rule_depth][item["id"]] += 1
            return idx
    return None


def crush_allows(move_pg):
    """
    for the given rule and utilized pg_osds,
    create a cheker that can verify osd replacements are valid.
    """

    # TODO asdf up or acting??
    # adjust when simulating cluster usage
    pg_osds = set(pg_osds_up[move_pg])

    pool_id = pool_from_pg(move_pg)
    pool = pools[pool_id]
    pool_size = pool["size"]
    shard_size = pool['pg_shard_size_avg']
    crushruleid = pool['crush_rule']
    rule = crushrules[crushruleid]

    # inverse crush check...

    # ruledepth -> allowed number of bucket reuse
    reuses_per_step = []
    fanout_cum = 1

    # calculate how often one bucket layer can be reused
    # this is the crush-constraint, set up by the rule
    for step in reversed(rule["steps"]):
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
            num += pool_size

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
    # fanout: step's num -> selections below bucket
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
    # * get trace from them
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
    root_name = None

    # osd -> crush-root-trace
    constraining_traces = dict()
    emit = False

    # rule_depth -> tree_depth to next rule (what tree layer is this rule step)
    # because "choose" steps may skip layers in the crush hierarchy
    rule_tree_depth = dict()

    # gather item usages by evaluating the crush rules
    for step in rule["steps"]:
        if step["op"] == "take":
            root_name = step["item_name"]

            # first step: try to find tracebacks for all osds that ends up in this root.
            constraining_traces = dict()

            for pg_osd in pg_osds:
                trace = trace_crush_root(pg_osd, root_name)
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

    return PGMoveChecker(move_pg, pg_osds, pool, constraining_traces, root_name, rule_tree_depth, reuses_per_step, item_uses)



# TODO create virtual cluster usage stats by actually summing all the pg sizes!
# then we can simulate movement and evaluate usage after each step

class PGMoveChecker:
    def __init__(self, pg, pg_osds, pool, constraining_traces, root_name, rule_tree_depth, reuses_per_step, item_uses):
        self.pg = pg
        self.pg_osds = pg_osds
        self.pool = pool
        self.constraining_traces = constraining_traces
        self.root_name = root_name
        self.rule_tree_depth = rule_tree_depth
        self.reuses_per_step = reuses_per_step
        self.item_uses = item_uses

        self.shard_size = pg_shardsize(self.pg)

        # gather available osd ids, by crush root
        for root_bucket, try_root_ids in bucket_roots:
            if root_bucket["name"] == root_name:
                root_ids = try_root_ids
                break

        if not root_ids:
            raise Exception(f"crush root {root} not known?")

        self.osd_candidates = {nodeid for nodeid in root_ids.keys() if nodeid >= 0}

    # produce a function that can verify if replacing old with new is valid
    def is_move_valid(self, old_osd, new_osd):
        """
        verify that the given new osd does not violate the
        crush rules' constraints of placement.
        """
        if new_osd in self.pg_osds:
            print(f"  invalid: {new_osd} in {self.pg_osds}")
            return False

        if new_osd not in self.osd_candidates:
            print(f"  invalid: {new_osd} not in same crush root")
            return False

        # create trace for the replacement candidate
        new_trace = trace_crush_root(new_osd, self.root_name)

        if new_trace is None:
            # probably not a compatible device class
            print(f"  no trace found for {new_osd}")
            return False

        # the trace we no longer consider (since we replace the osd)
        old_trace = self.constraining_traces[old_osd]

        print(f"reuses {self.reuses_per_step}")
        print(f"=>{new_osd}: {[item['id'] for item in new_trace]}")

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
                print(f"  invalid: {new_osd} violates crush: using {new_item} x {uses+1} > {use_max_allowed}")
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
            use_delta = 0
            if osd == osd_from:
                use_delta = -self.shard_size
            elif osd == osd_to:
                use_delta = self.shard_size

            osd_used = get_osd_usage(osd, use_delta)

            osd_used *= 100  # in percent :)

            osds_used.append(osd_used)

        return statistics.variance(osds_used)

    def get_pg_shardsize(self):
        return self.shard_size

    def filter_candidates(self, osdids):
        for osdid in osdids:
            if osdid not in self.osd_candidates:
                continue
            yield osdid



def get_osd_usage(osdid, use_delta=0):
    osd = osds[osdid]
    used = osd['device_used']
    size = osd['device_size']
    weight = osd['weight']
    used += use_delta
    return used / (size * weight)


if args.mode == 'balance':
    print("running pg balancer")

    only_poolids = None
    if args.only_poolid:
        only_poolids = {int(pool) for pool in args.only_poolid.split(",") if pool.strip()}
        print(f"only considering pools {only_poolids}")

    if args.only_crushclass:
        only_crushclasses = {cls.strip() for cls in args.only_crushclass.split(",") if cls.strip()}
        print(f"only considering crushclasses {only_crushclasses}")

        osd_candidates = {osd for osd in osds.keys() if crushclasses.get(osds) in only_crushclasses}
    else:
        osd_candidates = osds.keys()

    # start by taking the globally fullest OSD
    osds_usage_asc = list(sorted(osd_candidates, key=get_osd_usage))


    # we have to respect the crush rules for placement!
    # this is basically my approach to OSDMap::calc_pg_upmaps
    # and CrushWrapper::try_remap_rule python-implementation

    found = False

    # try to move the biggest pg from the fullest disk to the next suiting smaller disk
    # TODO: for multi-move-mode, only move one pg per osd!
    for osd_from in reversed(osds_usage_asc):
        if found:
            break

        # TODO: up or acting?
        # adjust when simulating cluster usage
        pg_candidates = osds[osd_from]["pgs_up"]
        pg_candidates_desc = sorted(pg_candidates, key=pg_shardsize, reverse=True)

        # TODO: sort those by shard-size?
        for move_pg in pg_candidates_desc:
            if found:
                break

            if only_poolids and pool_from_pg(move_pg) not in only_poolids:
                continue

            pg_move_state = crush_allows(move_pg)

            # check variance for this crush root
            variance_before = pg_move_state.get_placement_variance()

            # try the least full osd that's allowed by crush
            for osd_to in pg_move_state.filter_candidates(osds_usage_asc):
                print(f"TRY move {move_pg} osd.{osd_from} => osd.{osd_to}")

                if not pg_move_state.is_move_valid(osd_from, osd_to):
                    print(f"   BAD move {move_pg} osd.{osd_from} => osd.{osd_to}")
                    continue

                # check if the variance is decreasing
                new_variance = pg_move_state.get_placement_variance(osd_from, osd_to)

                if new_variance >= variance_before:
                    print(f"   BAD => variance not decreasing: {new_variance} not < {variance_before}")
                    continue

                new_pg_mapping = list()
                for osdid in pg_osds_up[move_pg]:
                    if osdid == osd_from:
                        osdid = osd_to
                    new_pg_mapping.append(osdid)

                print(f"   GOOD move {move_pg} osd.{osd_from} => osd.{osd_to}")
                print(f"    pg {move_pg} is then on {new_pg_mapping}")
                print(f"    => variance decreasing: {new_variance} < {variance_before}")
                print(f"   movement size {pprintsize(pg_move_state.get_pg_shardsize())}")

                # this is [(osdfrom, osdto), ...]
                current_upmaps = upmap_items.get(move_pg, [])

                prev_upmaps = " ".join(f"{mapping[0]} {mapping[1]}" for mapping in current_upmaps)

                print(f"% ceph osd pg-upmap-items {move_pg} {prev_upmaps}  {osd_from} {osd_to}")

                found = True
                break

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

            # header:
            print(f"{'osdid': >6} {crushclass}  {'devsize': >7}  {'util': >5} {'pg_num': >6}  pools")

            osd_entries = list()

            for osdid, props in osds.items():
                if args.use_weighted_utilization:
                    util_val = (props['device_used'] / (props['device_size'] * props['weight']) * 100)
                else:
                    util_val = props['utilization']

                if util_val < args.osd_fill_min:
                    continue

                util = "%.1f%%" % util_val
                crushclass = props['crush_class'].rjust(maxcrushclasslen)
                devsize = props['device_size']

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
                        count /= devsize / 1024 ** 4

                    pool_list[pool] = count

                osd_entries.append((osdid, crushclass, devsize, util, pg_num, pool_list))

            # default sort by osdid
            sort_func = lambda x: x[0]

            if args.sort_utilization:
                sort_func = lambda x: x[3]

            if args.sort_pg_count is not None:
                sort_func = lambda x: x[4].get(args.sort_pg_count, 0)

            for osdid, crushclass, devsize, util, pg_num, pool_pgs in sorted(osd_entries, key=sort_func):

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

                pool_list = ' '.join(pool_overview)
                print(f"{osdid: >6} {crushclass}  {pprintsize(devsize): >7}  {util: >5} {pg_num: >6}  {pool_list}")

    elif args.format == 'json':
        ret = {
            'pgstate': args.pgstate,
            'pools': pools,
            'osds': osds,
        }

        print(json.dumps(ret))

elif args.mode == 'showremapped':
    pass

else:
    raise Exception(f"unknown args mode {args.mode}")
