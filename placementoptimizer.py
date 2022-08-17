#!/usr/bin/env python3

"""
Ceph balancer.

(c) 2020-2022 Jonas Jelten <jj@sft.lol>
GPLv3 or later
"""

# some future TODOs to include in the optimization:
# maximum movement limits
# recommendations for pg num
# respect OMAP_BYTES and OMAP_KEYS for a pg
# don't touch a pool with decreasing pg_num
# a new mode that tries to eliminate upmap_items
# when considering an OSD for emptying, try pending PGs first
# ability to select pgs explicitly for better placement (move a backfill_tofull pg somewhere else)

# even "better" algorithm:
# get osdmap and crushmap
# calculate constraints weighted by device
# get current utilization weighted by device
# create z3 equation using these constraints
# transform result to upmap items


import argparse
import itertools
import json
import logging
import lzma
import shlex
import statistics
import subprocess
import datetime
from collections import defaultdict
from functools import lru_cache
from pprint import pformat, pprint


cli = argparse.ArgumentParser()

cli.add_argument("-v", "--verbose", action="count", default=0,
                 help="increase program verbosity")
cli.add_argument("-q", "--quiet", action="count", default=0,
                 help="decrease program verbosity")

sp = cli.add_subparsers(dest='mode')
sp.required=True

statep = argparse.ArgumentParser(add_help=False)
statep.add_argument("--state", "-s", help="load cluster state from this jsonfile")

gathersp = sp.add_parser('gather', help="only gather cluster information, i.e. generate a state file")
gathersp.add_argument("output_file", help="file to store cluster balancing information to")

showsp = sp.add_parser('show', parents=[statep])
showsp.add_argument('--only-crushclass',
                    help="only display devices of this crushclass")
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

remappsp = sp.add_parser('showremapped', parents=[statep])
remappsp.add_argument('--by-osd', action='store_true',
                      help="group the results by osd")
remappsp.add_argument('--osds',
                      help="only look at these osds when using --by-osd, comma separated")

balancep = sp.add_parser('balance', parents=[statep])
balancep.add_argument('--max-pg-moves', '-m', type=int, default=10,
                      help='maximum number of pg movements to find, default: %(default)s')
balancep.add_argument('--only-pool',
                      help='comma separated list of pool names to consider for balancing')
balancep.add_argument('--only-poolid',
                      help='comma separated list of pool ids to consider for balancing')
balancep.add_argument('--only-crushclass',
                      help='comma separated list of crush classes to balance')
balancep.add_argument('--osdused', choices=["delta", "shardsum"], default="shardsum",
                      help=('how is the osd usage predicted? default: %(default)s. '
                            "delta=adjusting the osd usage report by pending pg deltas, more accurate but doesn't account pending data deletion. "
                            "shardsum=estimate the usage by summing up all pg shardsizes, doesn't account PG metadata."))
balancep.add_argument('--pg-size-choice', choices=['largest', 'median', 'auto'],
                      default='largest',
                      help=('method to select a PG move candidate on a OSD based on its size. '
                            'auto tries to determine the best PG size by looking at '
                            'the currently emptiest OSD. '
                            'default: %(default)s'))
balancep.add_argument('--ensure-optimal-moves', action='store_true',
                      help='make sure that only movements which win full shardsizes are done')
balancep.add_argument('--ensure-variance-decrease', action='store_true',
                      help='make sure that only movements which decrease the fill rate variance are performed')
balancep.add_argument('--osdsize', choices=['device', 'weighted', 'crush'], default="crush",
                      help="what parameter to take for determining the osd size. weighted=devsize*weight, crush=crushweight*weight")
balancep.add_argument('--max-full-move-attempts', type=int, default=1,
                      help="when the fullest osd can't be emptied more, "
                           "try this many more osds in descending fullness order.")
balancep.add_argument('--source-osds',
                      help="only consider these osds as movement source, separated by ','")

pooldiffp = sp.add_parser('poolosddiff', parents=[statep])
pooldiffp.add_argument('--pgstate', choices=['up', 'acting'], default="acting",
                       help="what pg set to take, up or acting (default acting).")
pooldiffp.add_argument('pool1',
                       help="use this pool for finding involved osds")
pooldiffp.add_argument('pool2',
                       help="compare to this pool which osds are involved")

sp.add_parser('repairstats', parents=[statep])

stopbackfillsp = sp.add_parser("stopbackfills", parents=[statep])

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


class strlazy:
    """
    to be used like this: logging.debug("rolf %s", strlazy(lambda: do_something()))
    so do_something is only called when the debug message is actually printed
    do_something could also be an f-string.
    """
    def __init__(self, fun):
        self.fun = fun
    def __str__(self):
        return self.fun()


def jsoncall(cmd, swallow_stderr=False):
    if not isinstance(cmd, list):
        raise ValueError("need cmd as list")
    stderrval = subprocess.DEVNULL if swallow_stderr else None
    rawdata = subprocess.check_output(cmd, stderr=stderrval)
    return json.loads(rawdata.decode())


def pformatsize(size_bytes, commaplaces=1):
    prefixes = ((1, 'K'), (2, 'M'), (3, 'G'), (4, 'T'), (5, 'P'), (6, 'E'), (7, 'Z'))
    for exp, name in prefixes:
        if abs(size_bytes) >= 1024 ** exp and abs(size_bytes) < 1024 ** (exp + 1):
            new_size = size_bytes / 1024 ** exp
            fstring = "%%.%df%%s" % commaplaces
            return fstring % (new_size, name)

    return "%.1fB" % size_bytes


# to detect if imported state files are outdated
STATE_VERSION = 1

# use cluster state from a file
if args.mode != "gather" and args.state:
    logging.info(f"loading cluster state from file {args.state}...")
    with lzma.open(args.state) as hdl:
        CLUSTER_STATE = json.load(hdl)

    import_version = CLUSTER_STATE['stateversion']
    if import_version != STATE_VERSION:
        raise Exception(f"imported file stores state in version {import_version}, but we need {STATE_VERSION}")

else:
    logging.info(f"gathering cluster state via ceph api...")
    # this is shitty: this whole script depends on these outputs,
    # but they might be inconsistent, if the cluster had changes
    # between calls....
    # it would be really nice if we could "start a transaction"

    # ceph pg dump always echoes "dumped all" on stderr, silence that.
    CLUSTER_STATE = dict(
        stateversion=STATE_VERSION,
        timestamp=datetime.datetime.now().isoformat(),
        versions=jsoncall("ceph versions --format=json".split()),
        health_detail=jsoncall("ceph health detail --format=json".split()),
        osd_dump=jsoncall("ceph osd dump --format json".split()),
        pg_dump=jsoncall("ceph pg dump --format json".split(), swallow_stderr=True),
        osd_df_dump=jsoncall("ceph osd df --format json".split()),
        osd_df_tree_dump=jsoncall("ceph osd df tree --format json".split()),
        df_dump=jsoncall("ceph df detail --format json".split()),
        pool_dump=jsoncall("ceph osd pool ls detail --format json".split()),
        crush_dump=jsoncall("ceph osd crush dump --format json".split()),
        crush_class_osds=dict(),
    )

    crush_classes = jsoncall("ceph osd crush class ls --format json".split())
    for crush_class in crush_classes:
        class_osds = jsoncall(f"ceph osd crush class ls-osd {crush_class} --format json".split())
        if not class_osds:
            continue
        CLUSTER_STATE["crush_class_osds"][crush_class] = class_osds

    # check if the osdmap version changed meanwhile
    # => we'd have inconsistent state
    if CLUSTER_STATE['osd_dump']['epoch'] != jsoncall("ceph osd dump --format json".split())['epoch']:
        raise Exception("Cluster topology changed during information gathering (e.g. a pg changed state). "
                        "Wait for things to calm down and try again")


# to dump the state
if args.mode == 'gather':
    logging.info(f"cluster state dumped. now saving to {args.output_file}...")
    with lzma.open(args.output_file, "wt") as hdl:
        json.dump(CLUSTER_STATE, hdl, indent='\t')

    logging.warn(f"cluster state saved to {args.output_file}")
    exit(0)


pools = dict()                        # poolid => props
poolnames = dict()                    # poolname => poolid
crushrules = dict()                   # ruleid => props
crushclass_osds = defaultdict(set)    # crushclass => osdidset
crushclasses_usage = dict()           # crushclass => percent_used
osd_crushclass = dict()               # osdid => crushclass
maxpoolnamelen = 0
maxcrushclasslen = 0


for crush_class, class_osds in CLUSTER_STATE["crush_class_osds"].items():
    if not class_osds:
        continue

    crushclass_osds[crush_class].update(class_osds)
    for osdid in class_osds:
        osd_crushclass[osdid] = crush_class

    if len(crush_class) > maxcrushclasslen:
        maxcrushclasslen = len(crush_class)

    # there's more stats, but raw is probably ok
    class_df_stats = CLUSTER_STATE["df_dump"]["stats_by_class"][crush_class]
    crushclasses_usage[crush_class] = class_df_stats["total_used_raw_ratio"] * 100


for pool in CLUSTER_STATE["osd_dump"]["pools"]:
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

# current crush placement overrides
# map pgid -> [(from, to), ...]
upmap_items = dict()
for upmap_item in CLUSTER_STATE["osd_dump"]["pg_upmap_items"]:
    remaps = list()
    for remap in upmap_item["mappings"]:
        remaps.append((remap["from"], remap["to"]))

    upmap_items[upmap_item["pgid"]] = list(sorted(remaps))


ec_profiles = dict()
for ec_profile, ec_spec in CLUSTER_STATE["osd_dump"]["erasure_code_profiles"].items():
    ec_profiles[ec_profile] = {
        "data_chunks": int(ec_spec["k"]),
        "coding_chunks": int(ec_spec["m"]),
    }


for pool in CLUSTER_STATE["df_dump"]["pools"]:
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


# definition from `struct pg_pool_t`:
#  enum {
#      TYPE_REPLICATED = 1,     // replication
#      //TYPE_RAID4 = 2,   // raid4 (never implemented)
#      TYPE_ERASURE = 3,      // erasure-coded
#  };
def pool_repl_type(typeid):
    return {
        1: "repl",
        3: "ec",
    }[typeid]


for pool in CLUSTER_STATE["pool_dump"]:
    id = pool["pool_id"]
    pool_type = pool_repl_type(pool["type"])
    ec_profile = pool["erasure_code_profile"]

    pg_shard_size_avg = pools[id]["stored"] / pools[id]["pg_num"]

    if pool_type == "ec":
        pg_shard_size_avg /= ec_profiles[ec_profile]["data_chunks"]

    pools[id].update({
        "erasure_code_profile": ec_profile if pool_type == "ec" else None,
        "repl_type": pool_type,
        "pg_shard_size_avg": pg_shard_size_avg,
    })


for rule in CLUSTER_STATE["crush_dump"]["rules"]:
    id = rule['rule_id']
    name = rule['rule_name']
    steps = rule['steps']

    crushrules[id] = {
        'name': name,
        'steps': steps,
    }


def list_replace(iterator, search, replace):
    ret = list()
    for elem in iterator:
        if elem == search:
            elem = replace
        ret.append(elem)
    return ret


def pool_from_pg(pg):
    return int(pg.split(".")[0])


@lru_cache(maxsize=2**17)
def pg_ec_profile(pg):
    pool_id = pool_from_pg(pg)
    pool = pools[pool_id]
    return pool["erasure_code_profile"]


@lru_cache(maxsize=2**17)
def pg_is_ec(pg):
    pool_id = pool_from_pg(pg)
    pool = pools[pool_id]
    return pool["repl_type"] == "ec"


def get_remaps(pginfo):
    """
    given the pginfo structure, compare up and acting sets
    return which osds are source and target for pg movements.

    return [((osd_from, ...), (osd_to, ..)), ...]
    """
    up_osds = list_replace(pginfo["up"], 0x7fffffff, -1)
    acting_osds = list_replace(pginfo["acting"], 0x7fffffff, -1)

    is_ec = pg_is_ec(pginfo["pgid"])

    moves = list()
    if is_ec:
        for up_osd, acting_osd in zip(up_osds, acting_osds):
            if up_osd != acting_osd:
                moves.append(((acting_osd,), (up_osd,)))
    else:
        ups = set(up_osds)
        actings = set(acting_osds)
        from_osds = actings - ups
        to_osds = ups - actings

        moves.append(
            (
                tuple(sorted(from_osds)),
                tuple(sorted(to_osds)),
            )
        )

    return moves


# map osd -> pgs on it
osd_mappings = defaultdict(
    lambda: {'up': set(), 'primary': set(), 'acting': set()}
)

# map pg -> osds involved
pg_osds_up = defaultdict(set)
pg_osds_acting = defaultdict(set)

# osdid => {to: {pgid -> osdid}, from: {pgid -> osdid}}
osd_actions = defaultdict(lambda: defaultdict(dict))

# pg metadata
# pgid -> pg dump pgstats entry
pgs = dict()

for pginfo in CLUSTER_STATE["pg_dump"]["pg_map"]["pg_stats"]:
    if pginfo["state"] in ("unknown",):
        # skip pgs with no active osds
        continue

    pgid = pginfo["pgid"]
    up = pginfo["up"]
    acting = pginfo["acting"]
    primary = acting[0]

    pg_osds_up[pgid] = up
    pg_osds_acting[pgid] = acting

    osd_mappings[primary]['primary'].add(pgid)

    for osd in up:
        osd_mappings[osd]['up'].add(pgid)
    for osd in acting:
        osd_mappings[osd]['acting'].add(pgid)

    # track all remapped pgs
    pgstate = pginfo["state"].split("+")
    if "remapped" in pgstate:
        for osds_from, osds_to in get_remaps(pginfo):
            for osd_from, osd_to in zip(osds_from, osds_to):
                osd_actions[osd_from]["to"][pgid] = osd_to
                osd_actions[osd_to]["from"][pgid] = osd_from

    pgs[pgid] = pginfo


osds = dict()

for osd in CLUSTER_STATE["osd_df_dump"]["nodes"]:
    id = osd["id"]
    osds[id] = {
        "device_size": osd["kb"] * 1024,
        "device_used": osd["kb_used"] * 1024,
        "device_used_data": osd["kb_used_data"] * 1024,
        "device_used_meta": osd["kb_used_meta"] * 1024,
        "device_available": osd["kb_avail"] * 1024,
        "utilization": osd["utilization"],
        "crush_weight": osd["crush_weight"],
        "status": osd["status"],
    }


# osds used by a pool:
# pool_id -> {osdid}
pool_osds_up = defaultdict(set)
pool_osds_acting = defaultdict(set)

# gather which pgs are on what osd
# and which pools have which osds
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
        pool_osds_up[poolid].add(osdid)

    for pg in osd['acting']:
        poolid = int(pg.split('.', maxsplit=1)[0])
        osd_pools_acting.add(poolid)
        pgs_acting.add(pg)

        pg_count_acting[poolid] += 1
        pool_osds_acting[poolid].add(osdid)

    if osdid == 0x7fffffff:
        # the "missing" osds
        continue

    osds[osdid].update({
        'pools_up': list(sorted(osd_pools_up)),
        'pools_acting': list(sorted(osd_pools_acting)),
        'pg_count_up': pg_count_up,
        'pg_count_acting': pg_count_acting,
        'pg_num_up': len(pgs_up),
        'pgs_up': pgs_up,
        'pg_num_acting': len(pgs_acting),
        'pgs_acting': pgs_acting,
    })


for osd in CLUSTER_STATE["osd_dump"]["osds"]:
    osdid = osd["osd"]
    crushclass = osd_crushclass.get(osdid)

    osds[osdid].update({
        "weight": osd["weight"],
        "cluster_addr": osd["cluster_addr"],
        "public_addr": osd["public_addr"],
        "state": tuple(osd["state"]),
        'crush_class': crushclass,
    })


for osd_info in CLUSTER_STATE["pg_dump"]["pg_map"]["osd_stats"]:
    osds[osd_info['osd']]['stats'] = osd_info

osd_stats_sum = CLUSTER_STATE["pg_dump"]["pg_map"]["osd_stats_sum"]



# create the crush trees
buckets = CLUSTER_STATE["crush_dump"]["buckets"]

# bucketid -> bucket dict
bucket_ids_tmp = dict()

# all bucket ids of roots
bucket_root_ids = list()

for device in CLUSTER_STATE["crush_dump"]["devices"]:
    id = device["id"]
    assert id >= 0
    bucket_ids_tmp[id] = device

for bucket in buckets:
    id = bucket["id"]
    assert id < 0
    bucket_ids_tmp[id] = bucket

    # collect all root buckets
    if bucket["type_name"] == "root":
        bucket_root_ids.append(id)

    # get osd crush weights
    for item in bucket["items"]:
        item_id = item["id"]
        # it's an osd
        if item_id >= 0:
            # json-crushweight is in 64-gbyte blocks apparently
            size = (item["weight"] / 64) * 1024 ** 3
            osds[item_id].update({
                "crush_weight": size,
            })


# find osd host name
for node in CLUSTER_STATE["osd_df_tree_dump"]["nodes"]:
    if node['type'] == "host":
        for osdid in node['children']:
            osds[osdid]["host_name"] = node['name']


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


@lru_cache(maxsize=2 ** 14)
def trace_crush_root(osd, root_name):
    """
    in the given root, trace back all items from the osd up to the root
    """
    found = False
    for root_bucket, try_root_ids in bucket_roots:
        if root_bucket["name"] == root_name:
            root_ids = try_root_ids
            break

    if not root_ids:
        raise Exception(f"crush root {root_name} not known?")

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

        if root_ids[node_id]["name"] == root_name:
            found = True
            break

        node_id = root_ids[node_id]["parent"]

    if not found:
        raise Exception(f"could not find a crush-path from osd={osd} to {root_name!r}")

    topdown = list(reversed(bottomup))
    return topdown


@lru_cache(maxsize=2**20)
def get_pg_shardsize(pgid):
    pg_stats = pgs[pgid]['stat_sum']
    shard_size = pg_stats['num_bytes']
    shard_size += pg_stats['num_omap_bytes']

    ec_profile = pg_ec_profile(pgid)
    if ec_profile:
        shard_size /= ec_profiles[ec_profile]["data_chunks"]
        # omap is not supported on EC pools (yet)
        # when it is, check how the omap data is spread (replica or also ec?)

    return shard_size


def rule_for_pg(pg):
    """
    get the crush rule for a pg.
    """
    pool = pools[pool_from_pg(move_pg)]
    crushruleid = pool['crush_rule']
    return crushrules[crushruleid]


def root_uses_from_rule(rule, pool_size):
    """
    rule: crush rule id
    pool_size: number of osds in one pg

    return {root_name: choice_count} for the given crush rule.

    the choose-step nums are processed in order:
    val ==0: remaining_pool_size
    val < 0: remaining_pool_size - val
    val > 0: val
    """
    chosen = 0

    # rootname -> number of chooses for this root
    root_usages = defaultdict(int)

    root_candidate = None
    root_chosen = None
    root_use_num = None

    for step in rule["steps"]:
        if step["op"] == "take":
            root_candidate = step["item_name"]

        elif step["op"].startswith("choose"):
            # TODO: maybe "osd" type can be renamed, fetch it dynamically
            if root_candidate and (step["op"].startswith("chooseleaf")
                                   or step["type"] == "osd"):
                root_chosen = root_candidate
                root_use_num = step["num"]

        elif step["op"] == "emit":
            if root_chosen:
                if root_use_num == 0:
                    root_use_num = pool_size
                elif root_use_num < 0:
                    root_use_num = pool_size - root_use_num
                elif root_use_num > 0:
                    pass  # = root_use_num

                # limit to pool size
                if root_use_num > pool_size - chosen:
                    root_use_num = pool_size - chosen

                root_usages[root_chosen] += root_use_num
                chosen += root_use_num
                root_candidate = None
                root_chosen = None
                root_use_num = None
                if chosen == pool_size:
                    break

    if not root_usages:
        raise Exception(f"rule chooses no roots")

    return root_usages


def rootweights_from_rule(rule, pool_size):
    """
    given a crush rule and a pool size (involved osds in a pg),
    calculate the weights crush-roots are chosen for each pg.
    """
    root_usages = root_uses_from_rule(rule, pool_size)

    # normalize the weights:
    weight_sum = sum(root_usages.values())

    # TODO: handle `default` root
    #       -> distribute it to the other involved roots

    root_weights = dict()
    for root_name, root_usage in root_usages.items():
        root_weights[root_name] = root_usage / weight_sum

    return root_weights


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

    ret = set()

    for nodeid in root_ids.keys():
        if (nodeid >= 0 and
            osds[nodeid]['weight'] != 0 and
            osds[nodeid]['crush_weight'] != 0):

            ret.add(nodeid)

    return ret


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
        self.root_names = root_uses_from_rule(self.rule, self.pool_size).keys()

        # all available placement osds for this crush root
        self.osd_candidates = set()
        for root_name in self.root_names:
            for osdid in candidates_for_root(root_name):
                self.osd_candidates.add(osdid)

        self.pg_mappings = pg_mappings  # current pg->[osd] mapping state
        self.pg_osds = pg_mappings.get_mapping(move_pg)  # acting osds managing this pg

    def get_osd_candidates(self):
        """
        return all possible candidate OSDs for the PG to relocate.
        """
        return self.osd_candidates

    @staticmethod
    def use_item_type(trace, item_type, rule_depth, item_uses):
        """
        given a trace (part), walk forward, until a given item type is found.
        increase its use.
        """
        for idx, item in enumerate(trace):
            if item["type_name"] == item_type:
                item_id = item["id"]
                cur_item_uses = item_uses[rule_depth].get(item_id, 0)
                cur_item_uses += 1
                item_uses[rule_depth][item_id] = cur_item_uses
                return idx
        return None

    def prepare_crush_check(self):
        """
        perform precalculations for moving this pg
        """
        logging.debug(strlazy(lambda: f"prepare crush check for pg {self.pg} currently up={self.pg_osds}"))
        logging.debug(strlazy(lambda: f"rule:\n{pformat(self.rule)}"))

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

        logging.debug(strlazy(lambda: f"allowed reuses per rule step, starting at root: {pformat(reuses_per_step)}"))

        # for each depth, count how often items were used
        # rule_depth -> {itemid -> use count}
        item_uses = defaultdict(dict)

        # example: 2+2 ec -> size=4
        #
        # root        __________-9____________________________
        # rack: _____-7_______    _________-8_____      ___-10____
        # host: -1    -2    -3    -4    -5      -6      -11     -12
        # osd: 1 2 | 3 4 | 5 6 | 7 8 | 9 10 | 11 12 | 13 14 | 15 16
        #        ^     ^         ^     ^
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
        # traces from root down to osd:
        #   2: [-9, -7, -1, 2]
        #   4: [-9, -7, -2, 4]
        #   7: [-9, -8, -4, 7]
        #   9: [-9, -8, -5, 9]
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

        # did we encounter an emit?
        emit = False

        # collect trace for each osd.
        # osd -> crush-root-trace
        constraining_traces = dict()

        # how far down the crush hierarchy have we stepped
        # 1 = root depth
        tree_depth = 0

        # at what rule position is our processing
        # since we skip steps like set_chooseleaf_tries
        rule_depth = 0

        # rule_depth -> tree_depth to next rule (what tree layer is this rule step)
        # because "choose" steps may skip layers in the crush hierarchy
        rule_tree_depth = dict()

        current_item_type = None

        # gather item usages by evaluating the crush rules
        for step in self.rule["steps"]:
            if step["op"].startswith("set_"):
                # skip rule configuration steps
                continue

            logging.debug(strlazy(lambda: f"processing crush step {step} with tree_depth={tree_depth}, "
                                          f"rule_depth={rule_depth}, item_uses={item_uses}"))

            if step["op"] == "take":
                rule_root_name = step["item_name"]

                # should be known already since we fetch it the exact same way
                assert rule_root_name in self.root_names

                # first step: try to find tracebacks for all osds that ends up in this root.
                # we collect root-traces of all acting osds of the pg we wanna check for.
                constraining_traces = dict()

                for pg_osd in self.pg_osds:
                    trace = trace_crush_root(pg_osd, rule_root_name)
                    logging.debug(strlazy(lambda: f"   trace for {pg_osd:4d}: {trace}"))
                    if trace is None:
                        raise Exception(f"no trace found for {pg_osd} in {rule_root_name}")
                    constraining_traces[pg_osd] = trace
                    # the root was "used"
                    root_id = trace[0]["id"]
                    root_use = item_uses[rule_depth].setdefault(root_id, 0)
                    root_use += 1
                    item_uses[rule_depth][root_id] = root_use

                rule_tree_depth[rule_depth] = 0
                tree_depth = 0
                rule_depth += 1
                current_item_type = rule_root_name

            elif step["op"].startswith("choose"):
                if not constraining_traces:
                    raise Exception('no backtraces captured from rule (missing "take"?)')

                choose_type = step["type"]

                # find the new tree_depth by looking how far we need to step for the choosen next bucket

                for constraining_trace in constraining_traces.values():
                    steps_taken = self.use_item_type(constraining_trace[tree_depth:], choose_type, rule_depth, item_uses)
                    if steps_taken is None:
                        raise Exception(f"could not find item type {choose_type} "
                                        f"requested by rule step {step}")

                # how many layers we went down the tree
                rule_tree_depth[rule_depth] = tree_depth + steps_taken
                tree_depth += steps_taken
                rule_depth += 1
                current_item_type = choose_type

            elif step["op"] == "emit":
                # we've not yet reached osd level, so step down further
                for constraining_trace in constraining_traces.values():
                    steps_taken = self.use_item_type(constraining_trace[tree_depth:], "osd", rule_depth, item_uses)
                    if steps_taken is None:
                        raise Exception(f"could not find trace steps down to osd"
                                        f"requested by rule step {step}")

                rule_tree_depth[rule_depth] = tree_depth + steps_taken
                tree_depth += steps_taken
                rule_depth += 1

                # sanity checks lol
                assert len(rule_tree_depth) == rule_depth
                assert len(item_uses) == rule_depth
                for idx, step_reuses in enumerate(reuses_per_step):
                    for item, uses in item_uses[idx].items():
                        # uses may be <= since crush rules can emit more osds than the pool size needs
                        if uses > step_reuses:
                            print(f"rule:\n{pformat(self.rule)}")
                            print(f"reuses: {reuses_per_step}")
                            print(f"item_uses: {pformat(item_uses)}")
                            print(f"constraining_traces: {pformat(constraining_traces)}")
                            raise Exception(f"during emit, rule step {idx} item {item} was used {uses} > {step_reuses} expected")

                # only one emit supported so far
                if emit == True:
                    raise Exception("multiple emits in crush rule not yet supported")

                emit = True

            else:
                raise Exception(f"unknown crush operation encountered: {step['op']}")

        if not emit:
            raise Exception("uuh no emit seen?")

        if not constraining_traces:
            raise Exception("no tree traces gathered?")

        assert len(rule_tree_depth) == rule_depth
        assert len(item_uses) == rule_depth

        self.constraining_traces = constraining_traces  # osdid->crush-root-trace
        self.rule_tree_depth = rule_tree_depth  # rulestepid->tree_depth
        self.reuses_per_step = reuses_per_step  # rulestepid->allowed_item_reuses
        self.item_uses = item_uses  # rulestepid->{item->use_count}

        logging.debug(strlazy(lambda: f"crush check preparation done: rule_tree_depth={rule_tree_depth} item_uses={item_uses}"))

    def is_move_valid(self, old_osd, new_osd):
        """
        verify that the given new osd does not violate the
        crush rules' constraints of placement.
        """
        if new_osd in self.pg_osds:
            logging.debug(strlazy(lambda: f"   crush invalid: osd.{new_osd} in {self.pg_osds}"))
            return False

        if new_osd not in self.osd_candidates:
            logging.debug(strlazy(lambda: f"   crush invalid: osd.{new_osd} not in same crush root"))
            return False

        # TODO: figure out the crush root for the old_osd, and use it for new_osd too
        # for now, just assume there's only one root per rule...
        assert len(self.root_names) == 1
        old_osd_root = next(iter(self.root_names))

        # create trace for the replacement candidate
        new_trace = trace_crush_root(new_osd, old_osd_root)
        logging.debug(strlazy(lambda: f"   trace for {new_osd}: {new_trace}"))

        if new_trace is None:
            # probably not a compatible device class
            logging.debug(f"   crush: no trace found for {new_osd}")
            return False

        # the trace we no longer consider (since we replace the osd)
        old_trace = self.constraining_traces[old_osd]

        overuse = False
        for stepid, tree_stepwidth in self.rule_tree_depth.items():
            use_max_allowed = self.reuses_per_step[stepid]

            # as we would remove the old osd trace,
            # the item would no longer be occupied in the new trace
            old_item = old_trace[tree_stepwidth]["id"]
            # this trace now adds to the item uses:
            new_item = new_trace[tree_stepwidth]["id"]

            # how often is new_item used now?
            # if not used at all, it's not in the dict.
            uses = self.item_uses[stepid].get(new_item, 0)

            # we wanna use the new item now.
            # the use count doesn't increase when we already use the item on the current level
            if old_item != new_item:
                uses += 1

            # if we used it, it'd be violating crush
            if uses > use_max_allowed:
                logging.debug(strlazy(lambda: (f"   item reuse check fail: osd.{old_osd}@[{tree_stepwidth}]={old_item} -> "
                                               f"osd.{new_osd}@[{tree_stepwidth}]={new_item} x uses={uses} > max_allowed={use_max_allowed}")))
                overuse = True
                break
            else:
                logging.debug(strlazy(lambda: (f"   item reuse check ok:   osd.{old_osd}@[{tree_stepwidth}]={old_item} -> "
                                               f"osd.{new_osd}@[{tree_stepwidth}]={new_item} x uses={uses} <= max_allowed={use_max_allowed}")))


        return not overuse

    def get_placement_variance(self, osd_from=None, osd_to=None):
        """
        calculate the variance of weighted OSD usage
        for all OSDs that are candidates for this PG

        osd_from -> osd_to: how would the variance look, if
                            we had moved data.
        """

        if osd_from or osd_to:
            pg_shardsize = get_pg_shardsize(self.pg)

        osds_used = list()
        for osd in self.osd_candidates:
            delta = 0
            if osd == osd_from:
                delta = -pg_shardsize
            elif osd == osd_to:
                delta = pg_shardsize

            if osds[osd]['weight'] == 0 or osds[osd]['crush_weight'] == 0:
                # relative usage of weight 0 is impossible
                continue

            osd_used = self.pg_mappings.get_osd_usage(osd, add_size=delta)
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
    def __init__(self, pgs, osds, osd_candidates):

        # the "real" devices, just used for their "fixed" properties like
        # device size
        self.osds = osds

        # all possible osd candidates where we can map to (due to crush class)
        self.osd_candidates = osd_candidates

        # up state: osdid -> {pg, ...}
        self.osd_pgs_up = defaultdict(set)

        # acting state: osdid -> {pg, ...}
        self.osd_pgs_acting = defaultdict(set)

        # choose the up mapping, since we wanna optimize the "future" cluster
        # up pg mapping: pgid -> [up_osd, ...]
        self.pg_mappings = dict()

        for pg, pginfo in pgs.items():
            up_osds = pginfo["up"]
            self.pg_mappings[pg] = list(up_osds)

            for osdid in up_osds:
                self.osd_pgs_up[osdid].add(pg)

            acting_osds = pginfo["acting"]
            for osdid in acting_osds:
                self.osd_pgs_acting[osdid].add(pg)

        # pg->[(osd_from, osd_to), ...]
        self.remaps = dict()

        # osdid -> used kb, based on the osd-level utilization report
        self.osd_utilizations = dict()
        for osdid, osd in self.osds.items():
            # this is the current "acting" size.
            # and the parts of not-yet-fully-transferred up pgs
            osd_fs_used = osd['device_used']

            # we want the up-size, i.e. the osd utilization when all moves
            # would be finished.
            # to estimate it, we need to add the (partial) shards of (up - acting)
            # and remove the shards of (acting - up)
            # osd_fs_used already contains the size of partially transferred shards.
            # now we account for ongoing/planned transfers.

            # pgs that are transferred to the osd, so they will be acting soon(tm)
            for pg in (self.osd_pgs_up[osdid] - self.osd_pgs_acting[osdid]):
                shardsize = get_pg_shardsize(pg)
                pginfo = pgs[pg]

                pg_objs = pginfo["stat_sum"]["num_objects"]

                if pg_objs <= 0:
                    shardsize_already_transferred = 0
                else:
                    # for each move to be done, objects_misplaced+=pg_objs,
                    # thus we do /misplaced_shards below
                    pg_objs_misplaced = pginfo["stat_sum"]["num_objects_misplaced"]
                    pg_objs_degraded = pginfo["stat_sum"]["num_objects_degraded"]

                    # the pginfo statistics only provide us with pg-overall statistics
                    # so we need to figure out how much of that affects the current osdid
                    # -> see how many moves exist for the whole pg,
                    # and estimate the fraction due to movements with this osdid.

                    # the pg is moved and the source is missing
                    pg_degraded_moves = 0
                    # the pg is moved and the source exists
                    pg_misplaced_moves = 0
                    # this osd is the target of a pg move where the source is nonexistant
                    degraded_shards = 0
                    # this osd is the target of a pg move where the source does exist
                    misplaced_shards = 0

                    moves = get_remaps(pginfo)

                    # TODO in pginfo there's shard status, but that doesn't seem to contain
                    # sensible content?

                    for osds_from, osds_to in moves:
                        # -1 means in from -> move is degraded for EC
                        # for replica, len(from) < len(to) = degraded
                        if pg_is_ec(pg):
                            if -1 in osds_from:
                                pg_degraded_moves += len(osds_from)
                                if osdid in osds_to:
                                    degraded_shards += 1
                            else:
                                pg_misplaced_moves += len(osds_from)
                                if osdid in osds_to:
                                    misplaced_shards += 1
                        else:  # replica pg
                            if len(osds_to) == len(osds_from):
                                pg_misplaced_moves += len(osds_to)
                                if osdid in osds_to:
                                    misplaced_shards += 1
                            else:
                                pg_degraded_moves += len(osds_to) - len(osds_from)
                                if osdid in osds_to:
                                    degraded_shards += 1

                    # the following assumes that all shards of this pg are recovered at the same rate,
                    # when there's multiple from/to osds for this pg
                    osd_pg_objs_to_restore = 0
                    if degraded_shards > 0:
                        osd_pg_objs_degraded = pg_objs_degraded / pg_degraded_moves
                        osd_pg_objs_to_restore += osd_pg_objs_degraded * degraded_shards

                    if misplaced_shards > 0:
                        osd_pg_objs_misplaced = pg_objs_misplaced / pg_misplaced_moves
                        osd_pg_objs_to_restore += osd_pg_objs_misplaced * misplaced_shards

                    # adjust fs size by average object size times estimated restoration count.
                    # because this is kinda lame but it seems one can't get more info
                    # the balancer will work best if there are no more remapped PGs!
                    pg_obj_size = shardsize / pg_objs
                    pg_objs_transferred = pg_objs - osd_pg_objs_to_restore
                    if pg_objs_transferred < 0:
                        raise Exception(f"pg {pg} to be moved to osd.{osdid} is misplaced "
                                        f"with {pg_objs_transferred}<0 objects already transferred")

                    shardsize_already_transferred = int(pg_obj_size * pg_objs_transferred)

                missing_shardsize = shardsize - shardsize_already_transferred

                # add the estimated future shardsize
                osd_fs_used += missing_shardsize

            # pgs that are transferred off the osd
            for pg in (self.osd_pgs_acting[osdid] - self.osd_pgs_up[osdid]):
                osd_fs_used -= get_pg_shardsize(pg)

            self.osd_utilizations[osdid] = osd_fs_used
            logging.debug(strlazy(lambda: f"estimated {'osd.%s' % osdid: <8} usage: acting={pformatsize(osd['device_used'], 3)} up={pformatsize(osd_fs_used, 3)}"))

    def apply_remap(self, pg, osd_from, osd_to):
        """
        simulate a remap pg from one osd to another.
        this updates the mappings stored in this object.
        """

        self.osd_pgs_up[osd_from].remove(pg)
        self.osd_pgs_up[osd_to].add(pg)

        pg_mapping = self.pg_mappings[pg]
        did_remap = False
        for i in range(len(pg_mapping)):
            if pg_mapping[i] == osd_from:
                logging.debug(strlazy(lambda: f"recording move of pg={pg} from {osd_from}->{osd_to}"))
                pg_mapping[i] = osd_to
                did_remap = True
                break

        if not did_remap:
            raise Exception(f"did not find osd {osd_from} in pg {pg} mapping")

        # adjust the tracked sizes
        shard_size = get_pg_shardsize(pg)
        self.osd_utilizations[osd_from] -= shard_size
        self.osd_utilizations[osd_to] += shard_size

        self.get_osd_shardsize_sum.cache_clear()
        self.get_osd_usage.cache_clear()
        self.get_osd_space_allocated.cache_clear()

        # insert a new mapping tracker
        self.remaps.setdefault(pg, []).append((osd_from, osd_to))

    def get_osd_usages(self):
        """
        calculate {osdid -> usage percent}
        """
        # if an osd has a weight of 0, it's 100% full.
        return {osdid: self.get_osd_usage(osdid) for osdid in self.osd_candidates}

    def get_osd_usages_asc(self):
        """
        return [(osdid, usage in percent)] sorted ascendingly by usage
        """
        # from this mapping, calculate the weighted osd size,
        return list(sorted(self.get_osd_usages().items(), key=lambda osd: osd[1]))

    def get_mapping(self, pg):
        return list(self.pg_mappings[pg])

    def get_osd_pgs_up(self, osd):
        return set(self.osd_pgs_up[osd])

    def get_osd_pgs_acting(self, osd):
        return set(self.osd_pgs_acting[osd])

    def get_pg_move_candidates(self, osd):
        return PGCandidates(self, osd)

    @lru_cache(maxsize=2**18)
    def get_osd_shardsize_sum(self, osd):
        """
        calculate the osd usage by summing all the mapped (up) PGs shardsizes
        -> we can calculate a future size
        """
        used = 0

        for pg in self.get_osd_pgs_up(osd):
            used += get_pg_shardsize(pg)

        return used

    @lru_cache(maxsize=2**18)
    def get_osd_space_allocated(self, osdid):
        """
        return the osd usage reported by the osd, but with added shardsizes
        when pg movements were applied.
        """
        return self.osd_utilizations[osdid]

    def get_osd_weighted_size(self, osdid):
        """
        return the weighted OSD device size
        """

        osd = self.osds[osdid]
        size = osd['device_size']
        weight = osd['weight']

        return size * weight

    def get_osd_crush_weighted_size(self, osdid):
        """
        return the weighted OSD device size
        """

        osd = self.osds[osdid]
        size = osd['crush_weight']
        weight = osd['weight']

        return size * weight

    def get_osd_size(self, osdid):
        """
        return the osd size in bytes, depending on the size determination variant.
        """

        if args.osdsize == "device":
            osd_size = self.osds[osdid]['device_size']
        elif args.osdsize == "weighted":
            osd_size = self.get_osd_weighted_size(osdid)
        elif args.osdsize == "crush":
            osd_size = self.get_osd_crush_weighted_size(osdid)
        else:
            raise Exception(f"unknown osd weight method {args.osdsize!r}")

        return osd_size

    @lru_cache(maxsize=2**18)
    def get_osd_usage(self, osdid, add_size=0):
        """
        returns the occupied OSD space, by estimating the placed shards.

        another variant is add to the used data amount with add_size.
        """
        osd_size = self.get_osd_size(osdid)

        if osd_size == 0:
            raise Exception(f"getting relative usage of osd.{osdid} which has size 0 impossible")

        if args.osdused == "delta":
            used = self.get_osd_space_allocated(osdid)
        elif args.osdused == "shardsum":
            used = self.get_osd_shardsize_sum(osdid)
        else:
            raise Exception(f"unknown osd usage estimator: {args.osdused!r}")

        used += add_size

        # logging.debug(f"{osdid} {pformatsize(used)}/{pformatsize(osd_size)} = {used/osd_size * 100:.2f}%")

        # make it relative
        used /= osd_size
        used *= 100

        return used

    def get_upmaps(self):
        """
        get all applied mappings
        return {pgid: [(map_from, map_to), ...]}
        """
        # {pgid -> [(map_from, map_to), ...]}
        upmap_results = dict()

        for pgid in self.remaps.keys():
            upmap_results[pgid] = self.get_pg_upmap(pgid)

        return upmap_results

    def get_pg_upmap(self, pgid):
        """
        get all applied mappings for a pg
        return [(map_from, map_to), ...]
        """

        remaps_list = self.remaps.get(pgid, [])

        # remaps is [(osdfrom, osdto), ...], now osdfrom->osdto
        # these are the "new" remappings.
        # osdfrom->osdto
        new_remaps = dict(remaps_list)

        # merge new upmaps
        # i.e. (1, 2), (2, 3) => (1, 3)
        for new_from, new_to in remaps_list:
            other_to = new_remaps.get(new_to)
            if other_to is not None:
                new_remaps[new_from] = other_to
                del new_remaps[new_to]

        # current_upmaps are is [(osdfrom, osdto), ...]
        current_upmaps = upmap_items.get(pgid, [])

        # now, let's merge current_upmaps and remaps:

        # which of the current_upmaps to retain
        resulting_upmaps = list()

        # what remap-source-osds we already covered with merges
        merged_remaps = set()

        for current_from, current_to in current_upmaps:
            # do we currently map to a device, from which we move it again now?
            current_remapped_to = new_remaps.get(current_to)
            if current_remapped_to is not None:
                # this remap's source osd will now be merged
                merged_remaps.add(current_to)

                if current_from == current_remapped_to:
                    # skip current=(75->72), remapped=(72->75) leading to (75->75)
                    continue

                # transform e.g. current=197->188, remapped=188->261 to 197->261
                resulting_upmaps.append((current_from, current_remapped_to))
                continue

            resulting_upmaps.append((current_from, current_to))

        for new_from, new_to in new_remaps.items():
            if new_from in merged_remaps:
                continue
            resulting_upmaps.append((new_from, new_to))

        for new_from, new_to in resulting_upmaps:
            if new_from == new_to:
                raise Exception(f"somewhere something went wrong, we map {idpg} from osd.{new_from} to osd.{new_to}")

        return resulting_upmaps

    def osd_pool_pg_count(self, osdid):
        """
        return {pool -> pg_count} for an OSD
        """
        ret = defaultdict(lambda: 0)
        for pg in self.get_osd_pgs_up(osdid):
            ret[pool_from_pg(pg)] += 1

        return ret

    def pool_pg_count_ideal(self, poolid, candidate_osds):
        """
        return the ideal pg count for a poolid,
        given the candidate osd ids, expressed pgs/byte
        """

        pool = pools[poolid]
        pool_total_pg_count = pool['size'] * pool['pg_num']

        size_sum = 0

        for osdid in candidate_osds:
            size_sum += self.get_osd_size(osdid)

        # uuh somehow no osd had a size or all weights 0?
        assert size_sum > 0

        pgs_per_size = pool_total_pg_count / size_sum

        return pgs_per_size

    def osd_pool_pg_count_ideal(self, poolid, osdid, candidate_osds):
        """
        return the ideal pg count for a pool id for some osdid.
        """

        osd_size = self.get_osd_size(osdid)
        if osd_size == 0:
            return 0

        return self.pool_pg_count_ideal(poolid, candidate_osds) * osd_size

    def get_pool_max_avail(self, poolid):
        """
        given a pool id, predict how much space is available with the current mapping.
        similar to what `ceph df` tries to estimate
        """

        # TODO missing implementation details!

        pool = pools[poolid]
        pool_size = pool['size']
        pool_pg_num = pool['pg_num']
        pool_crushrule = crushrules[pool['crush_rule']]

        # cf. PGMap::get_rule_avail
        # (rootname, ruleweight)
        rootweights = rootweights_from_rule(pool_crushrule, pool_size)
        rootweightsum = sum(rootweights.values())
        osdid_candidates = set()
        for root_name in rootweights.keys():
            osdid_candidates |= candidates_for_root(root_name)

        avail = -1
        for osdid in osdid_candidates:
            if osds[osdid]['weight'] == 0 or osds[osdid]['crush_weight'] == 0:
                continue

            # calculation for `ceph df` max_avail:
            # weight = (crushweight[osdid] / deviceclass[osdid]_crushsum) * (rootweights[deviceclass_of[osdid]] / rootweightsum)
            #
            # better (since actual pg placement counts, not just hypothetical crush weights):
            # how many pgs of the given pool are on that osd
            # idea: have weight = (osd_weight * osd_pool_pg_count) / (osdid_candidates_crushweightsum * pool_pg_count)
            # TODO: include rule rootweights!
            osd_pool_pg_count = self.osd_pool_pg_count(osdid)[poolid]
            weight = (osd_pool_pg_count / sum_crushweight_osds_in_pool) / (pool_pg_num * pool_size / osdid_candidates_crushweightsum )

            unusable = osd_device_size[osdid] * (1.0 - full_ratio)
            devavail = max(0, osd_stats[osdid].kb_avail - unusable)
            # scale up device-available with it's fraction of the whole pool's crush weight
            proj = devavail / weight
            if proj < avail or avail < 0:
                avail = proj

        # scale down for replica/ec-usage
        max_avail = (avail / pool_use_rate)
        return max_avail


class PGShardProps:
    """
    info about one shard of a PG, i.e. the PG piece present on an OSD.
    """
    def __init__(self, size, remapped, upmap_count):
        self.size = size

        # bool: true, if the shard is not yet fully moved,
        # i.e. its in up, but not in acting of the osd
        # so we can prefer moving a pg that's not yet copied fully
        self.remapped = remapped

        # int: number of remappings the pg currently has
        # the more upmap items it has, the farther it is away from crush
        # -> the lower, the better.
        self.upmap_count = upmap_count

    def __lt__(self, other):
        """
        the "least" pg props entry is the one we try first.
        """
        # TODO: smarter selection of pg movement candidates
        # [x] order pgs by size, from big to small
        # [ ] order pgs by pg's num_omap_bytes
        # [x] prefer pgs that are remapped (up != acting)
        # [x] prefer pgs that don't have upmaps already to minimize "distance" to crush mapping
        # TODO: for those, change the move loops: loop src, loop dst, loop candidate pgs.
        # [ ] prefer pgs that have a upmap item and could be removed (tricky - since we have to know destination OSD)
        # [ ] prefer pgs that have optimal moves:
        #     prefer if predicted_target_usage < (current_target_usage + source_usage)/2
        # [ ] prefer pgs whose upmap item uses the current osd as mapping target (-> so we don't add more items)
        # [ ] perfer pgs that have bad pool balance (i.e. their count on this osd doesn't match the expected one) (we enforce that constraint anyway, but we may never pick a pg from another pool that would be useful to balance)
        # [ ] only consider source/dest osds of a pg that was remapped, not any osd of the whole pg

        # "most important" sorting first
        # so if the "important" path is the same, sort by other means

        # prefer pgs that are remapped
        if self.remapped != other.remapped:
            return self.remapped

        if self.upmap_count != other.upmap_count:
            return self.upmap_count < other.upmap_count

        # prefer biggest sized pgs
        # TODO: integrate the automatic size selection feature
        if self.size != other.size:
            return self.size > other.size

        return False

    def __str__(self):
        return f"size={pformatsize(self.size)} remapped={self.remapped} upmaps={self.upmap_count}"


class PGCandidates:
    """
    Generate movement candidates to empty the given osd.
    """

    def get_properties(self, pgid):
        return self.pg_properties[pgid]

    def __init__(self, pg_mappings, osd):
        up_pgs = pg_mappings.get_osd_pgs_up(osd)
        acting_pgs = pg_mappings.get_osd_pgs_acting(osd)
        remapped_pgs = up_pgs - acting_pgs

        self.pg_candidates = list()
        self.pg_properties = dict()

        # we can move up pgs away - they are on the osd or planned to be on it.
        for pgid in up_pgs:
            pg_pool = pool_from_pg(pgid)
            if only_poolids and pg_pool not in only_poolids:
                continue

            self.pg_candidates.append(pgid)

            self.pg_properties[pgid] = PGShardProps(
                size=get_pg_shardsize(pgid),
                remapped=(pgid in remapped_pgs),   # the shard is not yet fully moved
                upmap_count=len(pg_mappings.get_pg_upmap(pgid))
            )

        # best candidate first
        pg_candidates_desc = list(sorted(self.pg_candidates,
                                         key=lambda pg: self.pg_properties[pg]))

        # reorder potential pg_candidates by configurable approaces
        pg_choice_method = args.pg_size_choice
        pg_walk_anchor = None

        # TODO: move these choice methods also into the PGShardProps comparison function
        if pg_choice_method == 'auto':
            # choose the PG choice method based on
            # the utilization differences of the fullest and emptiest candidate OSD.

            # if we could move there, most balance would be created.
            # use this to guess the shard size to move.
            best_target_osd, best_target_osd_usage = pg_mappings.get_osd_usages()[-1]

            for idx, pg_candidate in enumerate(pg_candidates_desc):
                pg_candidate_size = self.pg_properties[pg_candidate].size
                # try the largest pg candidate first, and become smaller every step
                target_predicted_usage = pg_mappings.get_osd_usage(best_target_osd, add_size=pg_candidate_size)
                # if the optimal osd's predicted usage is < (target_usage + source_usage)/2 + limit
                # the whole PG fits (at least size-wise. the crush check comes later)
                # -> we try to move the biggest fitting PG first.
                mean_usage = (best_target_osd_usage + osd_from_used_percent) / 2
                if target_predicted_usage <= mean_usage:
                    logging.debug(strlazy(lambda: f"  START with PG candidate {pg_candidate} due to perfect size fit "
                                                  f"when moving to best osd.{best_target_osd}."))
                    pg_walk_anchor = idx
                    break

                logging.debug(strlazy(lambda:
                              f"  SKIP candidate size estimation of {pg_candidate} "
                              f"when moving to best osd.{best_target_osd}. "
                              f"predicted={target_predicted_usage:.3f}% > mean={mean_usage:.3f}%"))

            if pg_walk_anchor is None:
                # no PG fitted, i.e. no pg was small enough for an optimal move.
                # so we use the smallest pg (which is still too big by the estimation above)
                # to work towards the optimal mean usage to achieve ideal balance.
                # in other words, overshoot the usage mean between the OSDs
                # by the minimally possible amount.
                self.pg_candidates = reversed(pg_candidates_desc)

        elif pg_choice_method == 'largest':
            self.pg_candidates = pg_candidates_desc

        elif pg_choice_method == 'median':
            # here, we decide to move not the largest/smallest pg, but rather the median one.
            # order PGs around the median-sized one
            # [5, 4, 3, 2, 1] => [3, 4, 2, 5, 1]
            pg_candidates_desc_sizes = [self.pg_properties[pg].size for pg in pg_candidates_desc]
            pg_candidates_median = statistics.median_low(pg_candidates_desc_sizes)
            pg_walk_anchor = pg_candidates_desc_sizes.index(pg_candidates_median)

        else:
            raise Exception('unhandled shard choice')

        # if we have a walk anchor, alternate pg sizes around that anchor point
        # and build the pg_candidates that way.
        if pg_walk_anchor is not None:
            self.pg_candidates = list()
            hit_left = False
            hit_right = False
            for walk_jump in A001057():
                pg_walk_pos = pg_walk_anchor + walk_jump

                if hit_left and hit_right:
                    break
                elif pg_walk_pos < 0:
                    hit_left = True
                    continue
                elif pg_walk_pos >= len(pg_candidates_desc):
                    hit_right = True
                    continue

                pg = pg_candidates_desc[pg_walk_pos]
                self.pg_candidates.append(pg)

            assert len(self.pg_candidates) == len(pg_candidates_desc)

    def get_candidates(self):
        """
        return an iterable with pgids on the osd,
        in the order we should try moving them away.
        """
        return self.pg_candidates

    def get_size(self, pg):
        return self.pg_properties[pg].size

    def __len__(self):
        return len(self.pg_candidates)


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


def list_highlight(osdlist, changepos, colorcode):
    """
    highlight an element at given list position with an ansi color code.
    """
    ret = list()
    for idx, osd in enumerate(osdlist):
        if idx == changepos:
            ret.append(f"\x1b[{colorcode};1m{osd}\x1b[m")
        else:
            ret.append(str(osd))

    return f"[{', '.join(ret)}]"


def print_osd_usages(usagedict):
    """
    given a sorted (osdid, usage) iterable, print it for debugging.
    """
    return
    logging.debug("osd usages in ascending order:")
    for osdid, usage in usagedict:
        logging.debug("  usage of %s.osd.%s: %.2f%%", strlazy(lambda: osds[osdid]["host_name"]), osdid, usage)


def get_cluster_variance(crushclasses, pg_mappings):
    """
    get {crushclass -> variance} for given mapping
    """
    variances = dict()
    for crushclass in crushclasses:
        osd_usages = list()
        for osdid in crushclass_osds[crushclass]:
            if osds[osdid]['weight'] == 0 or osds[osdid]['crush_weight'] == 0:
                continue
            osd_usages.append(pg_mappings.get_osd_usage(osdid))

        if len(osd_usages) > 1:
            class_variance = statistics.variance(osd_usages)
        elif osd_usages:
            class_variance = osd_usages[0]
        else:
            raise Exception("no osds in crushclass, but variance requested")
        variances[crushclass] = class_variance

    return variances


if args.mode == 'balance':
    logging.info("running pg balancer")

    # this is basically my approach to OSDMap::calc_pg_upmaps
    # and a CrushWrapper::try_remap_rule python-implementation

    enabled_crushclasses = crushclass_osds.keys()

    if args.only_crushclass:
        enabled_crushclasses = {cls.strip() for cls in args.only_crushclass.split(",") if cls.strip()}
        logging.info(f"only considering crushclasses {enabled_crushclasses}")

    only_poolids = set()
    if args.only_poolid:
        only_poolids = {int(pool) for pool in args.only_poolid.split(",") if pool.strip()}

    if args.only_pool:
        only_poolids = {int(poolnames[pool.strip()]) for pool in args.only_pool.split(",") if pool.strip()}

    if only_poolids:
        logging.info(f"only considering pools {only_poolids}")
        enabled_crushclasses = set()
        for poolid in only_poolids:
            pool = pools[poolid]
            pool_size = pool['size']
            pool_crushrule = crushrules[pool['crush_rule']]
            for root_name in root_uses_from_rule(pool_crushrule, pool_size).keys():
                enabled_crushclasses |= {osds[osdid]['crush_class'] for osdid in candidates_for_root(root_name)}

    osd_candidates = set()
    for crushclass in enabled_crushclasses:
        osd_candidates |= crushclass_osds[crushclass]

    # filter osd candidates by weight, and remove weight=0
    osd_candidates = {osd for osd in osd_candidates if (osds[osd]['weight'] != 0 and osds[osd]['crush_weight'] != 0)}

    # we'll do all the optimizations in this mapping state
    pg_mappings = PGMappings(pgs, osds, osd_candidates)

    # to restrict source osds
    source_osds = None
    if args.source_osds:
        source_osds = [int(osdid) for osdid in args.source_osds.split(',')]

    # number of found remaps
    found_remap_count = 0
    # size of found remaps
    found_remap_size_sum = 0
    force_finish = False

    # start by taking the fullest OSD
    # this is updated when we do move a pg
    osd_usages_asc = pg_mappings.get_osd_usages_asc()
    print_osd_usages(osd_usages_asc)

    logging.info("current OSD fill rate per crushclasses:")
    for crushclass in enabled_crushclasses:
        usages = list()
        for osdid in crushclass_osds[crushclass]:
            if osds[osdid]['weight'] != 0 and osds[osdid]['crush_weight'] != 0:
                usages.append(pg_mappings.get_osd_usage(osdid))

        # fillaverage: calculated current osd usage
        # crushclassusage: used percent of all space provided by this crushclass
        fillaverage = statistics.mean(usages)
        fillmedian = statistics.median(usages)
        crushclass_usage = crushclasses_usage[crushclass]
        logging.info(f"  {crushclass}: average={fillaverage:.2f}%, median={fillmedian:.2f}%, without_placement_constraints={crushclass_usage:.2f}%")

    init_cluster_variance = get_cluster_variance(enabled_crushclasses, pg_mappings)
    cluster_variance = init_cluster_variance

    logging.info("cluster variance for crushclasses:")
    for crushclass, variance in init_cluster_variance.items():
        logging.info(f"  {crushclass}: {variance:.3f}")

    # TODO: track these min-max values per crushclass
    init_osd_min, init_osd_min_used = osd_usages_asc[0]
    init_osd_max, init_osd_max_used = osd_usages_asc[-1]
    osd_min = init_osd_min
    osd_min_used = init_osd_min_used
    osd_max = init_osd_max
    osd_max_used = init_osd_max_used
    logging.info(f"min osd.{init_osd_min} {init_osd_min_used:.3f}%")
    logging.info(f"max osd.{init_osd_max} {init_osd_max_used:.3f}%")

    while True:
        for osdid, usage in osd_usages_asc:
            if usage >= 100:
                logging.info(f"osd.{osdid} has calculated usage >= 100%: {usage}%")

        if found_remap_count >= args.max_pg_moves:
            logging.info("enough remaps found")
            break

        if force_finish:
            break

        found_remap = False

        unsuccessful_pools = set()

        source_attempts = 0
        last_attempt = -1

        # try to move the biggest pg from the fullest disk to the next suiting smaller disk
        for osd_from, osd_from_used_percent in reversed(osd_usages_asc):
            if found_remap or force_finish:
                break

            if source_osds is not None:
                if osd_from not in source_osds:
                    continue

            source_attempts += 1

            if source_attempts > args.max_full_move_attempts:
                logging.info(f"in descending full-order, couldn't empty osd.{last_attempt}, so we're done. "
                             f"if you want to try more often, set --max-full-move-attempts=$nr, this may unlock "
                             f"more balancing possibilities.")
                force_finish = True
                continue

            last_attempt = osd_from

            logging.debug("trying to empty osd.%s (%f %%)", osd_from, osd_from_used_percent)

            # these pgs are up on the source osd,
            # and are in the order of preference moving them away
            # TODO: it would be great if we could already know the destination osd candidates here
            # for better pg candidate selection.
            pg_candidates = pg_mappings.get_pg_move_candidates(osd_from)

            from_osd_pg_count = pg_mappings.osd_pool_pg_count(osd_from)

            from_osd_satisfied_pools = set()

            # now try to move the candidates to their possible destination
            # TODO: instead of having all candidates,
            #       always get the "next" one from a "smart" iterator
            #       there we can also feed in the current osd_usages_asc
            #       in order to "optimize" candidates by "anticipating" "target" "osds" "."
            #       even better, we can integrate the PGMoveChecker directly, and thus calculate
            #       target OSD candidates _within_ the pg candidate generation.
            for move_pg_idx, move_pg in enumerate(pg_candidates.get_candidates()):
                move_pg_shardsize = pg_candidates.get_size(move_pg)
                if found_remap or force_finish:
                    break

                pg_pool = pool_from_pg(move_pg)

                if pg_pool in from_osd_satisfied_pools:
                    logging.debug("SKIP pg %s since pool=%s-pgs are already below expected on osd.%s", move_pg, pg_pool, osd_from)
                    continue

                if pg_pool in unsuccessful_pools:
                    logging.debug("SKIP pg %s since pool (%s) can't be balanced more", move_pg, pg_pool)
                    continue

                logging.debug("TRY-0 moving pg %s (%s/%s) with %s from osd.%s", move_pg, move_pg_idx+1, len(pg_candidates), pformatsize(move_pg_shardsize), osd_from)

                try_pg_move = PGMoveChecker(pg_mappings, move_pg)
                pool_pg_count_ideal = pg_mappings.pool_pg_count_ideal(pg_pool, try_pg_move.get_osd_candidates())
                from_osd_pg_count_ideal = pool_pg_count_ideal * pg_mappings.get_osd_size(osd_from)

                # only move the pg if the source osd has more PGs of the pool than average
                # otherwise the regular balancer will fill this OSD again
                # with another PG (of the same pool) from somewhere
                if from_osd_pg_count[pg_pool] <= from_osd_pg_count_ideal:
                    from_osd_satisfied_pools.add(pg_pool)
                    logging.debug("  BAD => skipping pg %s since source osd.%s "
                                  "doesn't have too many of pool=%s (%s <= %s)",
                                  move_pg, osd_from, pg_pool, from_osd_pg_count[pg_pool], from_osd_pg_count_ideal)
                    continue
                logging.debug("  OK => taking pg %s from source osd.%s "
                              "since it has too many of pool=%s (%s > %s)",
                              move_pg, osd_from, pg_pool, from_osd_pg_count[pg_pool], from_osd_pg_count_ideal)

                # pre-filter PGs to rule out those that will for sure not gain any space
                pg_small_enough = False
                for osd_to in try_pg_move.get_osd_candidates():
                    target_predicted_usage = pg_mappings.get_osd_usage(osd_to, add_size=move_pg_shardsize)

                    # check if the target osd will be used less than the source
                    # after we moved the pg
                    if target_predicted_usage < osd_from_used_percent:
                        pg_small_enough = True
                        break

                if not pg_small_enough:
                    # the pg is so big, it would increase the fill % of all osd_tos more than osd_from is
                    # so we would increase the variance.
                    logging.debug("  BAD => skipping pg %s, since its not small enough", move_pg)
                    continue

                try_pg_move.prepare_crush_check()

                # check variance for this crush root
                variance_before = try_pg_move.get_placement_variance()

                # try the least full osd that's allowed by crush
                to_candidates = list(try_pg_move.filter_candidates(osd[0] for osd in osd_usages_asc))
                for osd_to_idx, osd_to in enumerate(to_candidates):
                    logging.debug("TRY-1 move %s osd.%s => osd.%s (%s/%s)", move_pg, osd_from, osd_to, osd_to_idx+1, len(to_candidates))

                    if osd_to == osd_from:
                        logging.debug(" BAD move to source OSD makes no sense")
                        continue

                    # in order to not fight the regular balancer, don't move the PG to a disk
                    # where the weighted pg count of this pool is already good
                    # otherwise the balancer will move a pg on the osd_from of this pool somewhere else
                    # i.e. don't be the +1
                    to_osd_pg_count = pg_mappings.osd_pool_pg_count(osd_to)
                    to_osd_pg_count_ideal = pool_pg_count_ideal * pg_mappings.get_osd_size(osd_to)
                    if to_osd_pg_count[pg_pool] >= to_osd_pg_count_ideal and not to_osd_pg_count_ideal < 1.0:
                        logging.debug(strlazy(lambda:
                                      f" BAD => osd.{osd_to} already has too many of pool={pg_pool} "
                                      f"({to_osd_pg_count[pg_pool]} >= {to_osd_pg_count_ideal})"))
                        continue
                    elif to_osd_pg_count_ideal >= 1.0:
                        logging.debug(strlazy(lambda:
                                      f" OK => osd.{osd_to} has too few of pool={pg_pool} "
                                      f"({to_osd_pg_count[pg_pool]} < {to_osd_pg_count_ideal})"))
                    else:
                        logging.debug(strlazy(lambda:
                                      f" OK => osd.{osd_to} doesn't have pool={pg_pool} yet "
                                      f"({to_osd_pg_count[pg_pool]} < {to_osd_pg_count_ideal})"))

                    # how full will the target be
                    target_predicted_usage = pg_mappings.get_osd_usage(osd_to, add_size=move_pg_shardsize)
                    source_usage = pg_mappings.get_osd_usage(osd_from)

                    # check that the destination osd won't be more full than the source osd
                    # but what if we're balanced very good already? wouldn't we allow this if the variance decreased anyway?
                    # maybe make it an optional flag because of this (--ensure-decreasing-usage)
                    if target_predicted_usage > source_usage:
                        logging.debug(strlazy(lambda:
                                      f" BAD target will be more full than source currently is: "
                                      f"osd.{osd_to} would have {target_predicted_usage:.3f}%, "
                                      f"and source's osd.{osd_from} currently is {source_usage:.3f}%"))
                        continue

                    # check if the movement size is nice
                    if args.ensure_optimal_moves:
                        # check if there's a target osd that will be filled less than mean of osd_to and osd_from after move
                        # predicted_target_usage < (target_usage + source_usage)/2 + limit
                        mean_usage = (pg_mappings.get_osd_usage(osd_to) + osd_from_used_percent) / 2
                        if target_predicted_usage > mean_usage:
                            logging.debug(f" BAD non-optimal size {move_pg} predicted={target_predicted_usage:.3f}% > mean={mean_usage:.3f}%")
                            continue

                    if not try_pg_move.is_move_valid(osd_from, osd_to):
                        logging.debug(f" BAD move {move_pg} osd.{osd_from} => osd.{osd_to}")
                        continue

                    # check if the variance is decreasing
                    new_variance = try_pg_move.get_placement_variance(osd_from, osd_to)

                    if new_variance >= variance_before:
                        # even if the variance increases, we ensure we do progress by the
                        # guaranteed usage rate decline
                        if args.ensure_variance_decrease:
                            logging.debug(f" BAD => variance not decreasing: {new_variance} not < {variance_before}")
                            continue

                    new_mapping_pos = None

                    prev_pg_mapping = pg_osds_up[move_pg]
                    new_pg_mapping = list()
                    for idx, osdid in enumerate(prev_pg_mapping):
                        if osdid == osd_from:
                            osdid = osd_to
                            new_mapping_pos = idx
                        new_pg_mapping.append(osdid)

                    pg_mappings.apply_remap(move_pg, osd_from, osd_to)

                    # refresh our global osd usage mapping
                    osd_usages_asc = pg_mappings.get_osd_usages_asc()
                    osd_min, osd_min_used = osd_usages_asc[0]
                    osd_max, osd_max_used = osd_usages_asc[-1]
                    cluster_variance = get_cluster_variance(enabled_crushclasses, pg_mappings)

                    # TODO: calculate win rate so we can predict the total free storage then
                    #       so we know how much actual free space our movement brought!
                    if new_variance > variance_before:
                        variance_op = ">"
                    elif new_variance < variance_before:
                        variance_op = "<"
                    else:
                        variance_op = "=="

                    logging.info(f"  SAVE move {move_pg} osd.{osd_from} => osd.{osd_to} ")
                    logging.info(f"    props: {pg_candidates.get_properties(move_pg)}")
                    logging.debug(strlazy(lambda: f"    pg {move_pg} was on {list_highlight(prev_pg_mapping, new_mapping_pos, 31)}"))
                    logging.debug(strlazy(lambda: f"    pg {move_pg} now on {list_highlight(new_pg_mapping, new_mapping_pos, 32)}"))
                    logging.info(f"    => variance new={new_variance} {variance_op} {variance_before}=old")
                    logging.info(f"    new min osd.{osd_min} {osd_min_used:.3f}%")
                    logging.info(f"        max osd.{osd_max} {osd_max_used:.3f}%")
                    logging.info(f"    new cluster variance:")
                    for crushclass, variance in cluster_variance.items():
                        logging.info(f"      {crushclass}: {variance:.3f}")

                    found_remap = True
                    found_remap_count += 1
                    # TODO: calculate this though the PGMappings remaps
                    found_remap_size_sum += move_pg_shardsize
                    break

                if not found_remap:
                    # we tried all osds to place this pg,
                    # so the shardsize is just too big
                    # if pg_size_choice is auto, we try to avoid this PG anyway,
                    # but if we still end up here, it means the choices for moves are really
                    # becoming tight.
                    unsuccessful_pools.add(pg_pool)

    # show results!
    print_osd_usages(osd_usages_asc)
    logging.info(80*"-")
    logging.info(f"generated {found_remap_count} remaps.")
    logging.info(f"total movement size: {pformatsize(found_remap_size_sum)}.")
    logging.info(80*"-")
    logging.info("old cluster variance per crushclass:")
    for crushclass, variance in init_cluster_variance.items():
        logging.info(f"  {crushclass}: {variance:.3f}")
    logging.info(f"old min osd.{init_osd_min} {init_osd_min_used:.3f}%")
    logging.info(f"old max osd.{init_osd_max} {init_osd_max_used:.3f}%")
    logging.info(80*"-")
    logging.info(f"new min osd.{osd_min} {osd_min_used:.3f}%")
    logging.info(f"new max osd.{osd_max} {osd_max_used:.3f}%")
    logging.info(f"new cluster variance:")
    for crushclass, variance in cluster_variance.items():
        logging.info(f"  {crushclass}: {variance:.3f}")
    logging.info(80*"-")
    for pg, upmaps in pg_mappings.get_upmaps().items():
        if upmaps:
            upmap_str = " ".join([f"{osd_from} {osd_to}" for (osd_from, osd_to) in upmaps])
            print(f"ceph osd pg-upmap-items {pg} {upmap_str}")
        else:
            print(f"ceph osd rm-pg-upmap-items {pg}")


elif args.mode == 'show':
    if args.format == 'plain':
        poolname = 'name'.ljust(maxpoolnamelen)
        print()
        print(f"{'poolid': >6} {poolname} {'type': <7} {'size': >5} {'min': >3} {'pg_num': >6} {'stored': >7} {'used': >7} {'avail': >7} {'shrdsize': >8} crush")

        # default, sort by pool id
        sort_func = lambda x: x[0]

        if args.sort_shardsize:
            sort_func = lambda x: x[1]['pg_shard_size_avg']

        stored_sum = 0
        stored_sum_class = defaultdict(int)
        used_sum = 0
        used_sum_class = defaultdict(int)

        for poolid, poolprops in sorted(pools.items(), key=sort_func):
            poolname = poolprops['name'].ljust(maxpoolnamelen)
            repl_type = poolprops['repl_type']
            if repl_type == "ec":
                profile = ec_profiles[poolprops['erasure_code_profile']]
                repl_type = f"ec{profile['data_chunks']}+{profile['coding_chunks']}"

            crushruleid = poolprops['crush_rule']
            crushrule = crushrules[crushruleid]
            crushrulename = crushrule['name']
            rootweights = rootweights_from_rule(crushrule, poolprops["size"])

            rootweights_ppl = list()
            for crushroot, weight in rootweights.items():
                stored_sum_class[crushroot] += poolprops['stored'] * weight
                used_sum_class[crushroot] += poolprops['used'] * weight
                rootweights_ppl.append(f"{crushroot}*{weight:.3f}")

            size = poolprops['size']
            min_size = poolprops['min_size']
            pg_num = poolprops['pg_num']
            stored_sum += poolprops['stored']
            used_sum += poolprops['used']
            stored = pformatsize(poolprops['stored'])  # used data excl metadata
            used = pformatsize(poolprops['used'])  # raw usage incl metastuff
            avail = pformatsize(poolprops['store_avail'])
            shard_size = pformatsize(poolprops['pg_shard_size_avg'])
            rootweights_pp = ",".join(rootweights_ppl)
            print(f"{poolid: >6} {poolname} {repl_type: <7} {size: >5} {min_size: >3} {pg_num: >6} {stored: >7} {used: >7} {avail: >7} {shard_size: >8} {crushruleid}:{crushrulename} {rootweights_pp: >12}")

        for crushroot in stored_sum_class.keys():
            crushclass_usage = ""
            crush_tree_class = crushroot.split("~")
            if len(crush_tree_class) >= 2:
                crush_class = crush_tree_class[1]
                crushclass_usage = f"{crushclasses_usage[crush_class]:.3f}%"

            print(f"{crushroot: <14}    {' ' * maxpoolnamelen} {' ' * 13} "
                  f"{pformatsize(stored_sum_class[crushroot], 2): >7} "
                  f"{pformatsize(used_sum_class[crushroot], 2): >7}"
                  f"{crushclass_usage: >10}")

        print(f"sum    {' ' * maxpoolnamelen} {' ' * 24} {pformatsize(stored_sum, 2): >7} {pformatsize(used_sum, 2): >7}")

        if args.osds:
            if args.only_crushclass:
                maxcrushclasslen = len(args.only_crushclass)
            maxcrushclasslen = min(maxcrushclasslen, len('class'))
            crushclassheader = 'cls'.rjust(maxcrushclasslen)

            osd_entries = list()

            for osdid, props in osds.items():
                hostname = props.get('host_name', '?')
                crushclass = props['crush_class']

                if args.only_crushclass:
                    if crushclass != args.only_crushclass:
                        continue

                if props['device_size'] == 0:
                    util_val = 0
                elif args.use_shardsize_sum:
                    used = 0
                    if args.pgstate == 'up':
                        placed_pgs = props['pgs_up']
                    elif args.pgstate == 'acting':
                        placed_pgs = props['pgs_acting']
                    else:
                        raise Exception("unknown pgstate")

                    for pg in placed_pgs:
                        used += get_pg_shardsize(pg)

                else:
                    used = props['device_used']

                if props['device_size'] == 0:
                    util_val = 0
                else:
                    util_val = 100 * used / props['device_size']

                weight_val = props['weight']
                cweight = props['crush_weight']

                if args.use_weighted_utilization:
                    if weight_val == 0:
                        util_val = 0
                    else:
                        util_val /= weight_val

                if util_val < args.osd_fill_min:
                    continue

                devsize = props['device_size']

                if args.pgstate == 'up':
                    pg_count = props.get('pg_count_up', {})
                    pg_num = props.get('pg_num_up', 0)
                elif args.pgstate == 'acting':
                    pg_count = props.get('pg_count_acting', dict())
                    pg_num = props.get('pg_num_acting', 0)
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

                class_val = crushclass.rjust(maxcrushclasslen)

                osd_entries.append((osdid, hostname, class_val, devsize, weight_val, cweight, util_val, pg_num, pool_list))

            # default sort by osdid
            sort_func = lambda x: x[0]

            if args.sort_utilization:
                sort_func = lambda x: x[6]

            if args.sort_pg_count is not None:
                sort_func = lambda x: x[7].get(args.sort_pg_count, 0)

            # header:
            print()
            print(f"{'osdid': >6} {'hostname': >10} {crushclassheader} {'devsize': >7} {'weight': >6} {'cweight': >7} {'util': >5} {'pg_num': >6}  pools")
            for osdid, hostname, crushclass, devsize, weight, cweight, util, pg_num, pool_pgs in sorted(osd_entries, key=sort_func):

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
                util = "%.1f%%" % util
                weight = "%.2f" % weight

                pool_list_str = ' '.join(pool_overview)
                print(f"{osdid: >6} {hostname: >10} {crushclass} {pformatsize(devsize): >7} {weight: >6} {pformatsize(cweight): >7} {util: >5} {pg_num: >6}  {pool_list_str}")

    elif args.format == 'json':
        ret = {
            'pgstate': args.pgstate,
            'pools': pools,
            'osds': osds,
        }

        print(json.dumps(ret))

elif args.mode == 'showremapped':
    # pgid -> move information
    pg_move_status = dict()

    for pg, pginfo in pgs.items():
        pgstate = pginfo["state"].split("+")
        if "remapped" in pgstate:

            moves = list()
            osd_move_count = 0
            for osds_from, osds_to in get_remaps(pginfo):
                froms = ','.join(str(osd) for osd in osds_from)
                tos = ','.join(str(osd) for osd in osds_to)
                moves.append(f"{froms}->{tos}")
                osd_move_count += len(osds_to)

            # multiply with move-count since each shard remap moves all objects again
            objs_total = pginfo["stat_sum"]["num_objects"] * osd_move_count
            objs_misplaced = pginfo["stat_sum"]["num_objects_misplaced"]
            objs_degraded = pginfo["stat_sum"]["num_objects_degraded"]
            objs_to_restore = objs_misplaced + objs_degraded

            if objs_total > 0:
                progress = 1 - (objs_to_restore / objs_total)
            else:
                progress = 1
            progress *= 100

            state = "backfill" if "backfilling" in pgstate else "waiting"
            if "backfill_toofull" in pgstate:
                state = "toofull"
            if "degraded" in pgstate:
                state = f"degraded+{state:<8}"
            else:
                state = f"         {state:<8}"

            pg_move_size = get_pg_shardsize(pg)
            pg_move_size_pp = pformatsize(pg_move_size)

            pg_move_status[pg] = {
                "state": state,
                "objs_total": objs_total,
                "objs_todo": objs_total - objs_to_restore,
                "size": pg_move_size,
                "sizepp": pg_move_size_pp,
                "progress": progress,
                "moves": moves,
            }

    if args.by_osd:
        if args.osds:
            osdids = [int(osdid) for osdid in args.osds.split(",")]
            osdlist = sorted((osdid, osd_actions[osdid]) for osdid in osdids)
        else:
            osdlist = sorted(osd_actions.items())

        for osdid, actions in osdlist:
            if osdid != -1:
                osd_d_size = osds[osdid]['device_size']
                osd_d_size_pp = pformatsize(osd_d_size, 2)

                osd_d_used = osds[osdid]['device_used']
                osd_d_used_pp = pformatsize(osd_d_used, 2)

                osd_c_size = osds[osdid]['crush_weight'] * osds[osdid]['weight']
                osd_c_size_pp = pformatsize(osd_c_size, 2)

                if osd_d_size == 0:
                    osd_d_fullness = 0
                else:
                    osd_d_fullness = osd_d_used / osd_d_size * 100

                if osd_c_size == 0:
                    osd_c_fullness = 0
                else:
                    osd_c_fullness = osd_d_used / osd_c_size * 100

                osdname = f"osd.{osdid}"
                fullness = (f"  drive={osd_d_fullness:.1f}% {osd_d_used_pp}/{osd_d_size_pp}"
                            f"  crush={osd_c_fullness:.1f}% {osd_d_used_pp}/{osd_c_size_pp}")

            else:
                osdname = "osd.missing"
                fullness = ""

            sum_to = len(actions['to'])
            sum_from = len(actions['from'])
            sum_data_to = sum((pg_move_status[pg]['size'] for pg in actions["to"].keys()))
            sum_data_from = sum((pg_move_status[pg]['size'] for pg in actions["from"].keys()))
            sum_data_delta = (sum_data_from - sum_data_to)
            sum_data_to_pp = pformatsize(sum_data_to, 2)
            sum_data_from_pp = pformatsize(sum_data_from, 2)
            sum_data_delta_pp = pformatsize(sum_data_delta, 2)

            print(f"{osdname}: {osds[osdid]['host_name']}  =>{sum_to} {sum_data_to_pp} <={sum_from} {sum_data_from_pp}"
                  f" (\N{Greek Capital Letter Delta}{sum_data_delta_pp}) {fullness}")

            for pg, to_osd in actions["to"].items():
                pgstatus = pg_move_status[pg]
                print(f"  ->{pg: <6} {pgstatus['state']} {pgstatus['sizepp']: >6} {osdid: >4}->{to_osd: <4} {pgstatus['objs_todo']} of {pgstatus['objs_total']}, {pgstatus['progress']:.1f}%")
            for pg, from_osd in actions["from"].items():
                pgstatus = pg_move_status[pg]
                print(f"  <-{pg: <6} {pgstatus['state']} {pgstatus['sizepp']: >6} {osdid: >4}<-{from_osd: <4} {pgstatus['objs_todo']} of {pgstatus['objs_total']}, {pgstatus['progress']:.1f}%")
            print()

    else:
        for pg, pgmoveinfo in pg_move_status.items():
            state = pgmoveinfo['state']
            move_size_pp = pgmoveinfo['sizepp']
            objs_total = pgmoveinfo['objs_total']
            objs_todo = pgmoveinfo['objs_todo']
            progress = pgmoveinfo['progress']
            move_actions = ';'.join(pgmoveinfo['moves'])
            print(f"pg {pg: <6} {state} {move_size_pp: >6}: {objs_todo} of {objs_total}, {progress:.1f}%, {move_actions}")

elif args.mode == 'poolosddiff':
    if args.pgstate == "up":
        pool_osds = pool_osds_up
    elif args.pgstate == "acting":
        pool_osds = pool_osds_acting
    else:
        raise Exception("unknown pgstate {args.pgstate!r}")

    p1_id = poolnames[args.pool1]
    p2_id = poolnames[args.pool2]

    p1_osds = pool_osds[p1_id]
    p2_osds = pool_osds[p2_id]

    ret = {
        "pool1": args.pool1,
        "pool2": args.pool2,
        "union": p1_osds | p2_osds,
        "intersect": p1_osds & p2_osds,
        "p1-p2": p1_osds - p2_osds,
        "p2-p1": p2_osds - p1_osds,
    }
    pprint(ret)

elif args.mode == 'repairstats':
    print(f"repaired reads globally: {osd_stats_sum['num_shards_repaired']}")
    for osdid, osdinfos in osds.items():
        repairs = osdinfos["stats"]["num_shards_repaired"]
        if repairs != 0:
            print(f"repaired on {osdid:>6}: {repairs}")

elif args.mode == 'stopbackfills':
    active_backfill = []
    waiting_backfill= []

    for pg, pginfo in pgs.items():
        pgstate = pginfo["state"].split("+")
        moves_backward = {}
        if "remapped" in pgstate:
            moves_backward = {(osd_to[0],osd_from[0]) for osd_from,osd_to in get_remaps(pginfo)}
            moves_backward = moves_backward.union(set(upmap_items.get(pg, {})))

            if "backfilling" in pgstate:
                active_backfill.append(
                    "ceph osd pg-upmap-items %s %s"
                    %(
                        pg,
                        " ".join(f"{x} {y}" for x,y in moves_backward),
                    )
                )
            elif "backfill_wait" in pgstate:
                waiting_backfill.append(
                    "ceph osd pg-upmap-items %s %s"
                    %(
                        pg,
                        " ".join(f"{x} {y}" for x,y in moves_backward),
                    )
                )

    if active_backfill:
        print("----  ACTIVE  ----")
        for item in active_backfill:
            print(item)

    if waiting_backfill:
        print("----  WAIT  ----")
        for item in waiting_backfill:
            print(item)

else:
    raise Exception(f"unknown args mode {args.mode}")
