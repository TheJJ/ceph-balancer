#!/usr/bin/env python3

"""
Ceph balancer.

(c) 2020-2023 Jonas Jelten <jj@sft.lol>
GPLv3 or later
"""


import argparse
import itertools
import json
import logging
import lzma
import shlex
import statistics
import subprocess
import datetime
from enum import Enum
from collections import defaultdict
from functools import lru_cache
from itertools import chain, zip_longest
from pprint import pformat, pprint
from typing import Optional, Callable


def parse_args():
    cli = argparse.ArgumentParser()

    cli.add_argument("-v", "--verbose", action="count", default=0,
                    help="increase program verbosity")
    cli.add_argument("-q", "--quiet", action="count", default=0,
                    help="decrease program verbosity")
    cli.add_argument('--osdsize', choices=['device', 'weighted', 'crush'], default="crush",
                     help=("what parameter to take for determining the osd size. default: %(default)s. "
                           "device=device_size, weighted=devsize*weight, crush=crushweight*weight"))

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
    showsp.add_argument('--original-avail-prediction', action='store_true',
                        help=("display what ceph reports as available size prediction - "
                              "ceph's value is unfortunately wrong for multi-take crush rules, "
                              "and it doesn't respect the current pg placements at all. it just uses crush weights."))
    showsp.add_argument('--show-max-avail', action='store_true',
                        help="show how much space would be available if the pool had infinity many pgs")
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
    balancep.add_argument('--pg-choice', choices=['largest', 'median', 'auto'],
                          default='largest',
                          help=('method to select a PG move candidate on a OSD based on its size. '
                                'auto tries to determine the best PG size by looking at '
                                'the currently emptiest OSD. '
                                'default: %(default)s'))
    balancep.add_argument('--osdused', choices=["delta", "shardsum"], default="shardsum",
                          help=('how is the osd usage predicted during simulation? default: %(default)s. '
                                "delta=adjust the builtin osd usage report by in-move pg deltas, more accurate but doesn't account pending data deletion. "
                                "shardsum=estimate the usage by summing up all pg shardsizes, doesn't account PG metadata."))
    balancep.add_argument('--osdfrom', choices=["fullest", "limiting", "alternate"], default="alternate",
                          help=('how to determine the source osd for a movement? default: %(default)s. '
                                "fullest=start with the fullest osd (by percent of device size). "
                                "limiting=start with the fullest osd that actually limits the usable pool space. "
                                "alternate=alternate between limiting and fullest devices"))
    balancep.add_argument('--ensure-optimal-moves', action='store_true',
                          help='make sure that only movements which win full shardsizes are done')
    balancep.add_argument('--ensure-variance-decrease', action='store_true',
                          help='make sure that only movements which decrease the fill rate variance are performed')
    balancep.add_argument('--max-move-attempts', type=int, default=2,
                          help=("current source osd can't be emptied more, "
                                "try this many more other osds candidates to empty. default: %(default)s"))
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

    args = cli.parse_args()
    return args



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


class OSDSizeMethod(Enum):
    """
    how to determine the OSD size
    """
    CRUSH = 1  # use the crush size
    DEVICE = 2  # use the device size
    WEIGHTED = 3  # weighted device size


class OSDUsedMethod(Enum):
    """
    how to determine the OSD usage size during simulation.
    we don't know what the OSD will actually do for movement and cleanup,
    so we have options to choose from how to estimate the new usage.
    """
    # adjusting the reported osd usage report by adding fractions of currently-in-move pg sizes.
    # more accurate but doesn't account pending data deletion.
    DELTA = 1
    # estimate the usage by summing up all pg shardsizes,
    # doesn't account PG metadata.
    SHARDSUM = 2


class OSDFromChoiceMethod(Enum):
    """
    how to choose a osd to move data from
    """
    FULLEST = 1  # use the fullest osd
    LIMITING = 2  # use devices limiting the pool available space
    ALTERNATE = 3 # alternate between limiting and fullest devices


class PGChoiceMethod(Enum):
    """
    how to select a pg for movement
    """
    # take the largest pg from the best source osd
    LARGEST = 1

    # take the median pg size from the best source osd
    MEDIAN = 2

    # determine the best pg size automatically by looking at the ideal space needed on the emptiest osd.
    AUTO = 3


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


def list_replace(iterator, search, replace):
    ret = list()
    for elem in iterator:
        if elem == search:
            elem = replace
        ret.append(elem)
    return ret


def pool_from_pg(pg):
    return int(pg.split(".")[0])


def bucket_fill(id, bucket_info, parent_id=None):
    """
    returns the list of all child buckets for a given id
    plus for each of those, their children.
    """
    bucket = bucket_info[id]

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
        child = bucket_info[child_item["id"]]
        cid = child["id"]
        if cid < 0:
            new_nodes, new_ids = bucket_fill(cid, bucket_info, id)
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


class ClusterState:
    # to detect if imported state files are incompatible
    STATE_VERSION = 1

    def __init__(self, statefile, osdsize_method: OSDSizeMethod = OSDSizeMethod.CRUSH):
        self.state = dict()
        self.load(statefile)

        self.osdsize_method = osdsize_method

    def load(self, statefile: Optional[str]):
        # use cluster state from a file
        if statefile:
            logging.info(f"loading cluster state from file {statefile}...")
            with lzma.open(statefile) as hdl:
                self.state = json.load(hdl)

            import_version = self.state['stateversion']
            if import_version != self.STATE_VERSION:
                raise Exception(f"imported file stores state in version {import_version}, but we need {self.STATE_VERSION}")

        else:
            logging.info(f"gathering cluster state via ceph api...")
            # this is shitty: this whole script depends on these outputs,
            # but they might be inconsistent, if the cluster had changes
            # between calls....
            # it would be really nice if we could "start a transaction"

            self.state = dict(
                stateversion=self.STATE_VERSION,
                timestamp=datetime.datetime.now().isoformat(),
                versions=jsoncall("ceph versions --format=json".split()),
                health_detail=jsoncall("ceph health detail --format=json".split()),
                osd_dump=jsoncall("ceph osd dump --format json".split()),
                # ceph pg dump always echoes "dumped all" on stderr, silence that.
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
                self.state["crush_class_osds"][crush_class] = class_osds

            # check if the osdmap version changed meanwhile
            # => we'd have inconsistent state
            if self.state['osd_dump']['epoch'] != jsoncall("ceph osd dump --format json".split())['epoch']:
                raise Exception("Cluster topology changed during information gathering (e.g. a pg changed state). "
                                "Wait for things to calm down and try again")

    def dump(self, output_file):
        logging.info(f"cluster state dumped. now saving to {output_file}...")
        with lzma.open(output_file, "wt") as hdl:
            json.dump(self.state, hdl, indent='\t')

        logging.warn(f"cluster state saved to {output_file}")


    @lru_cache(maxsize=2**17)
    def pg_is_ec(self, pg):
        pool_id = pool_from_pg(pg)
        pool = self.pools[pool_id]
        return pool["repl_type"] == "ec"


    @lru_cache(maxsize=2**18)
    def get_pg_shardsize(self, pgid):
        pg_stats = self.pgs[pgid]['stat_sum']
        shard_size = pg_stats['num_bytes']
        shard_size += pg_stats['num_omap_bytes']

        pool_id = pool_from_pg(pgid)
        pool = self.pools[pool_id]
        ec_profile = pool["erasure_code_profile"]
        if ec_profile:
            shard_size /= self.ec_profiles[ec_profile]["data_chunks"]
            # omap is not supported on EC pools (yet)
            # when it is, check how the omap data is spread (replica or also ec?)

        return shard_size


    def get_remaps(self, pginfo):
        """
        given the pginfo structure, compare up and acting sets
        return which osds are source and target for pg movements.

        return [((osd_from, ...), (osd_to, ..)), ...]
        """
        up_osds = list_replace(pginfo["up"], 0x7fffffff, -1)
        acting_osds = list_replace(pginfo["acting"], 0x7fffffff, -1)

        is_ec = self.pg_is_ec(pginfo["pgid"])

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


    def preprocess(self):
        """
        pre-fetches and combines various information from the cluster state
        """

        self.pools = dict()                        # poolid => props
        self.poolnames = dict()                    # poolname => poolid
        self.crushrules = dict()                   # ruleid => props
        self.crushclass_osds = defaultdict(set)    # crushclass => osdidset
        self.crushclasses_usage = dict()           # crushclass => percent_used
        self.osd_crushclass = dict()               # osdid => crushclass
        self.ec_profiles = dict()                  # erasure coding profile names

        # current crush placement overrides
        # map pgid -> [(from, to), ...]
        self.upmap_items = dict()

        # map pg -> osds involved
        self.pg_osds_up = defaultdict(set)
        self.pg_osds_acting = defaultdict(set)

        # osdid => {to: {pgid -> osdid}, from: {pgid -> osdid}}
        self.osd_actions = defaultdict(lambda: defaultdict(dict))

        # pg metadata
        # pgid -> pg dump pgstats entry
        self.pgs = dict()

        # osdid -> osdinfo
        self.osds = dict()

        # osdid -> device size in bytes
        self.osd_devsize = dict()

        # osds used by a pool:
        # pool_id -> {osdid}
        self.pool_osds_up = defaultdict(set)
        self.pool_osds_acting = defaultdict(set)

        # all crush bucket root ids
        self.bucket_roots = list()

        # what full percentage OSDs no longer accept data
        self.full_ratio = self.state["osd_dump"]["full_ratio"]

        for crush_class, class_osds in self.state["crush_class_osds"].items():
            if not class_osds:
                continue

            self.crushclass_osds[crush_class].update(class_osds)
            for osdid in class_osds:
                self.osd_crushclass[osdid] = crush_class

            # there's more stats, but raw is probably ok
            class_df_stats = self.state["df_dump"]["stats_by_class"][crush_class]
            self.crushclasses_usage[crush_class] = class_df_stats["total_used_raw_ratio"] * 100

        # longest poolname's length
        self.max_poolname_len = 0

        for pool in self.state["osd_dump"]["pools"]:
            id = pool["pool"]
            name = pool["pool_name"]

            if len(name) > self.max_poolname_len:
                self.max_poolname_len = len(name)

            self.pools[id] = {
                'name': name,
                'crush_rule': pool["crush_rule"],
                'pg_num': pool["pg_num"],  # current pgs before merge
                'pgp_num': pool["pg_placement_num"],  # actual placed pg count
                'pg_num_target': pool["pg_num_target"],  # target pg num
                'size': pool["size"],
                'min_size': pool["min_size"],
            }

            self.poolnames[name] = id

        for upmap_item in self.state["osd_dump"]["pg_upmap_items"]:
            remaps = list()
            for remap in upmap_item["mappings"]:
                remaps.append((remap["from"], remap["to"]))

            self.upmap_items[upmap_item["pgid"]] = list(sorted(remaps))


        for ec_profile, ec_spec in self.state["osd_dump"]["erasure_code_profiles"].items():
            self.ec_profiles[ec_profile] = {
                "data_chunks": int(ec_spec["k"]),
                "coding_chunks": int(ec_spec["m"]),
            }


        for pool in self.state["df_dump"]["pools"]:
            id = pool["id"]
            self.pools[id].update({
                "stored": pool["stats"]["stored"],  # actually stored data
                "objects": pool["stats"]["objects"],  # number of pool objects
                "used": pool["stats"]["bytes_used"],  # including redundant data
                "store_avail": pool["stats"]["max_avail"],  # available storage amount
                "percent_used": pool["stats"]["percent_used"],
                "quota_bytes": pool["stats"]["quota_bytes"],
                "quota_objects": pool["stats"]["quota_objects"],
            })


        for pool in self.state["pool_dump"]:
            id = pool["pool_id"]
            pool_type = pool_repl_type(pool["type"])
            ec_profile = pool["erasure_code_profile"]

            pg_shard_size_avg = self.pools[id]["stored"] / self.pools[id]["pg_num"]

            if pool_type == "ec":
                profile = self.ec_profiles[ec_profile]
                pg_shard_size_avg /= profile["data_chunks"]
                blowup_rate = profile["data_chunks"] / (profile["data_chunks"] + profile["coding_chunks"])

            elif pool_type == "repl":
                blowup_rate = pool["size"]

            else:
                raise Exception(f"unknown {pool_type=}")

            self.pools[id].update({
                "erasure_code_profile": ec_profile if pool_type == "ec" else None,
                "repl_type": pool_type,
                "pg_shard_size_avg": pg_shard_size_avg,
                "blowup_rate": blowup_rate,
            })

        for pool_stat in self.state["pg_dump"]["pg_map"]["pool_stats"]:
            id = pool_stat["poolid"]

            self.pools[id].update({
                "num_objects": pool_stat["stat_sum"]["num_objects"],
                "num_object_copies": pool_stat["stat_sum"]["num_object_copies"],
                "num_objects_degraded": pool_stat["stat_sum"]["num_objects_degraded"],
                "num_objects_misplaced": pool_stat["stat_sum"]["num_objects_misplaced"],
                "num_omap_bytes": pool_stat["stat_sum"]["num_omap_bytes"],
                "num_omap_keys": pool_stat["stat_sum"]["num_omap_keys"],
                "num_bytes": pool_stat["stat_sum"]["num_bytes"],
            })


        for rule in self.state["crush_dump"]["rules"]:
            id = rule['rule_id']
            name = rule['rule_name']
            steps = rule['steps']

            self.crushrules[id] = {
                'name': name,
                'steps': steps,
            }


        # map osd -> pgs on it
        osd_mappings = defaultdict(
            lambda: {'up': set(), 'primary': set(), 'acting': set()}
        )

        for pginfo in self.state["pg_dump"]["pg_map"]["pg_stats"]:
            if pginfo["state"] in ("unknown",):
                # skip pgs with no active osds
                continue

            pgid = pginfo["pgid"]
            self.pgs[pgid] = pginfo

            up = pginfo["up"]
            acting = pginfo["acting"]
            primary = acting[0]

            self.pg_osds_up[pgid] = up
            self.pg_osds_acting[pgid] = acting

            osd_mappings[primary]['primary'].add(pgid)

            for osd in up:
                osd_mappings[osd]['up'].add(pgid)
            for osd in acting:
                osd_mappings[osd]['acting'].add(pgid)

            # track all remapped pgs
            pgstate = pginfo["state"].split("+")
            if "remapped" in pgstate:
                for osds_from, osds_to in self.get_remaps(pginfo):
                    for osd_from, osd_to in zip(osds_from, osds_to):
                        self.osd_actions[osd_from]["to"][pgid] = osd_to
                        self.osd_actions[osd_to]["from"][pgid] = osd_from


        for osd in self.state["osd_df_dump"]["nodes"]:
            id = osd["id"]
            self.osds[id] = {
                "device_size": osd["kb"] * 1024,
                "device_used": osd["kb_used"] * 1024,
                "device_used_data": osd["kb_used_data"] * 1024,
                "device_used_meta": osd["kb_used_meta"] * 1024,
                "device_available": osd["kb_avail"] * 1024,
                "utilization": osd["utilization"],
                "crush_weight": osd["crush_weight"],
                "status": osd["status"],
            }
            self.osd_devsize[id] = osd["kb"] * 1024


        # gather which pgs are on what osd
        # and which pools have which osds
        for osdid, osdpgs in osd_mappings.items():
            osd_pools_up = set()
            osd_pools_acting = set()

            pgs_up = set()
            pgs_acting = set()

            pg_count_up = defaultdict(int)
            pg_count_acting = defaultdict(int)

            for pg in osdpgs['up']:
                poolid = pool_from_pg(pg)
                osd_pools_up.add(poolid)
                pgs_up.add(pg)

                pg_count_up[poolid] += 1
                self.pool_osds_up[poolid].add(osdid)

            for pg in osdpgs['acting']:
                poolid = pool_from_pg(pg)
                osd_pools_acting.add(poolid)
                pgs_acting.add(pg)

                pg_count_acting[poolid] += 1
                self.pool_osds_acting[poolid].add(osdid)

            if osdid == 0x7fffffff:
                # the "missing" osds
                continue

            self.osds[osdid].update({
                'pools_up': list(sorted(osd_pools_up)),
                'pools_acting': list(sorted(osd_pools_acting)),
                'pg_count_up': pg_count_up,
                'pg_count_acting': pg_count_acting,
                'pg_num_up': len(pgs_up),
                'pgs_up': pgs_up,
                'pg_num_acting': len(pgs_acting),
                'pgs_acting': pgs_acting,
            })


        for osd in self.state["osd_dump"]["osds"]:
            osdid = osd["osd"]
            crushclass = self.osd_crushclass.get(osdid)

            self.osds[osdid].update({
                "weight": osd["weight"],
                "cluster_addr": osd["cluster_addr"],
                "public_addr": osd["public_addr"],
                "state": tuple(osd["state"]),
                'crush_class': crushclass,
            })


        for osd_info in self.state["pg_dump"]["pg_map"]["osd_stats"]:
            self.osds[osd_info['osd']]['stats'] = osd_info


        # store osd host name
        for node in self.state["osd_df_tree_dump"]["nodes"]:
            if node['type'] == "host":
                for osdid in node['children']:
                    self.osds[osdid]["host_name"] = node['name']


        # create the crush trees
        buckets = self.state["crush_dump"]["buckets"]

        # bucketid -> bucket dict
        # bucketid is a negative number
        bucket_info = dict()

        # all bucket ids of roots
        bucket_root_ids = list()

        # assign devices to bucket ids
        for device in self.state["crush_dump"]["devices"]:
            id = device["id"]
            assert id >= 0
            bucket_info[id] = device

        for bucket in buckets:
            id = bucket["id"]
            assert id < 0
            bucket_info[id] = bucket

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
                    self.osds[item_id].update({
                        "crush_weight": size,
                    })

        # populare all bucket roots
        for root_bucket_id in bucket_root_ids:
            bucket_tree, bucket_ids = bucket_fill(root_bucket_id, bucket_info)
            self.bucket_roots.append((bucket_tree, bucket_ids))

        del bucket_info

    @lru_cache(maxsize=2 ** 14)
    def candidates_for_root(self, root_name):
        """
        get the all osds where a crush rule could place shards.
        returns {osdid: osdweight}
        """

        for root_bucket, try_root_ids in self.bucket_roots:
            if root_bucket["name"] == root_name:
                root_ids = try_root_ids
                break

        if not root_ids:
            raise Exception(f"crush root {root} not known?")

        ret = dict()

        for nodeid in root_ids.keys():
            if (nodeid >= 0 and
                self.osds[nodeid]['weight'] != 0 and
                self.osds[nodeid]['crush_weight'] != 0):

                ret[nodeid] = self.osds[nodeid]['weight'] * self.osds[nodeid]['crush_weight']

        return ret

    @lru_cache(maxsize=2 ** 14)
    def candidates_for_pool(self, poolid):
        """
        get all osd candidates for a given pool (due to its crush rule).
        returns {osdid: osdweight_summed_and_normalized_to_weight_sum}
        """

        pool = self.pools[poolid]
        pool_size = pool['size']
        pool_pg_num = pool['pg_num']
        pool_crushrule = self.crushrules[pool['crush_rule']]

        # cf. PGMap::get_rule_avail
        # {rootname -> relative_root_selection_weight}
        rootweights = rootweights_from_rule(pool_crushrule, pool_size)
        root_weight_sum = sum(rootweights.values())
        osd_weights = defaultdict(lambda: 0.0)

        crush_sum = 0.0
        for root_name, root_weight in rootweights.items():
            root_weight_fraction = root_weight / root_weight_sum

            # for each crush root chosen in the pool's rule, get the candidates
            candidates = self.candidates_for_root(root_name)

            # accumulate osd weights by how often they can be chosen from a crush rule.
            for osdid, osdweight in candidates.items():
                # apply crush rule weight (because the osd would be chosen more often due to the rule)
                osdweight *= root_weight_fraction
                crush_sum += osdweight
                osd_weights[osdid] += osdweight

        for osdid in osd_weights.keys():
            osd_weights[osdid] /= crush_sum

        return osd_weights


    def get_crushclass_osds(self, crushclass, skip_zeroweight : bool = False):
        """
        get all osdids belonging to given crush class.
        if weight/crushweight is 0, it can be skipped.
        """
        for osdid in self.crushclass_osds[crushclass]:
            if (skip_zeroweight and
                (self.osds[osdid]['weight'] == 0 or
                 self.osds[osdid]['crush_weight'] == 0)):
                continue
            yield osdid

    def get_osd_weighted_size(self, osdid):
        """
        return the weighted OSD device size
        """

        osd = self.osds[osdid]
        size = self.osd_devsize[osdid]
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

    def get_osd_size(self, osdid, adjust_full_ratio=False):
        """
        return the osd size in bytes, depending on the size determination variant.
        takes into account the size loss due to "full_ratio"
        """

        if self.osdsize_method == OSDSizeMethod.DEVICE:
            osd_size = self.osd_devsize[osdid]
        elif self.osdsize_method == OSDSizeMethod.WEIGHTED:
            osd_size = self.get_osd_weighted_size(osdid)
        elif self.osdsize_method == OSDSizeMethod.CRUSH:
            osd_size = self.get_osd_crush_weighted_size(osdid)
        else:
            raise Exception(f"unknown osd weight method {self.osdsize_method!r}")

        if adjust_full_ratio:
            osd_size *=  self.full_ratio

        return osd_size

    def pool_pg_shard_count_ideal(self, poolid, candidate_osds):
        """
        return the ideal pg count for a poolid,
        given the candidate osd ids, expressed pgs/byte
        """

        pool = self.pools[poolid]
        pool_total_pg_count = pool['size'] * pool['pg_num']

        size_sum = 0

        for osdid in candidate_osds:
            size_sum += self.get_osd_size(osdid)

        # uuh somehow no osd had a size or all weights 0?
        assert size_sum > 0

        pgs_per_size = pool_total_pg_count / size_sum

        return pgs_per_size

    def osd_pool_pg_shard_count_ideal(self, poolid, osdid, candidate_osds):
        """
        return the ideal pg count for a pool id for some osdid.
        """

        osd_size = self.get_osd_size(osdid)
        if osd_size == 0:
            return 0

        return self.pool_pg_shard_count_ideal(poolid, candidate_osds) * osd_size



@lru_cache(maxsize=2 ** 14)
def trace_crush_root(osd, root_name, cluster):
    """
    in the given root, trace back all items from the osd up to the root
    """
    found = False
    for root_bucket, try_root_ids in cluster.bucket_roots:
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



def root_uses_from_rule(rule, pool_size):
    """
    rule: crush rule id
    pool_size: number of osds in one pg

    return {root_name: choice_count}, [root_for_first_emitted_osd, next_root, ...]
    for the given crush rule.

    the choose-step nums are processed in order:
    val ==0: remaining_pool_size
    val < 0: remaining_pool_size - val
    val > 0: val
    """
    chosen = 0

    # rootname -> number of chooses for this root
    root_usages = defaultdict(int)
    root_order = list()

    root_candidate = None
    root_chosen = None
    root_use_num = None

    for step in rule["steps"]:
        if step["op"] == "take":
            root_candidate = step["item_name"]

        elif step["op"].startswith("choose"):
            # TODO: maybe "osd" type can be renamed? then we need to fetch it dynamically
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
                    pass  # so we use root_use_num

                # limit to pool size
                root_use_num = min(pool_size - chosen, root_use_num)
                root_usages[root_chosen] += root_use_num

                root_order.extend([root_chosen] * root_use_num)

                chosen += root_use_num
                root_candidate = None
                root_chosen = None
                root_use_num = None
                if chosen == pool_size:
                    break

    if not root_usages:
        raise Exception(f"rule chooses no roots")

    return root_usages, root_order


def rootweights_from_rule(rule, pool_size):
    """
    given a crush rule and a pool size (involved osds in a pg),
    calculate the weights crush-roots are chosen for each pg.

    returns {root_name -> relative_choice_weight_to_all_root_choices}
    """
    # root_name -> choice_count
    root_usages, _ = root_uses_from_rule(rule, pool_size)

    # normalize the weights:
    weight_sum = sum(root_usages.values())

    root_weights = dict()
    for root_name, root_usage in root_usages.items():
        root_weights[root_name] = root_usage / weight_sum

    return root_weights


def get_max_reuses(rule, pool_size):
    """
    generate a list of item reuses per rule step.
    one list entry per root take.
    take_index -> take_step -> max_reuses
    -> e.g. [[4, 2, 1, 1], [2, 1, 1]]
       for [[root, rack, server, osd], [otherroot, server, osd]]
    """

    reuses = []

    reuses_for_take: Optional[list] = None
    fanout_cum = 1

    # calculate how often one bucket layer can be reused
    # this is the crush-constraint, set up by the rule
    for idx, step in enumerate(rule["steps"]):
        if step["op"] == "take":
            if reuses_for_take is not None:
                reuses.append(list(reversed(reuses_for_take)))

            reuses_for_take = []
            fanout_cum = 1
            num = 1
        elif step["op"].startswith("choose"):
            num = step["num"]
        elif step["op"] == "emit":
            num = 1
        else:
            continue

        reuses_for_take.append(fanout_cum)

        if num <= 0:
            numprev = 0
            num += pool_size

        fanout_cum *= num

    reuses.append(list(reversed(reuses_for_take)))

    return reuses


class PGMoveChecker:
    """
    for the given rule and utilized pg_osds,
    create a checker that can verify osd replacements are valid.
    """

    def __init__(self, pg_mappings, move_pg):

        self.cluster = pg_mappings.cluster

        # which pg to relocate
        self.pg = move_pg
        self.pg_mappings = pg_mappings  # current pg->[osd] mapping state
        self.pg_osds = pg_mappings.get_mapping(move_pg)  # acting osds managing this pg

        self.pool = self.cluster.pools[pool_from_pg(move_pg)]
        self.pool_size = self.pool["size"]
        self.rule = self.cluster.crushrules[self.pool['crush_rule']]

        logging.debug(strlazy(lambda: (f"movecheck for pg {move_pg} (poolsize={self.pool_size})")))

        # crush root names and usages for this pg
        root_uses, root_order = root_uses_from_rule(self.rule, self.pool_size)

        # map osd -> crush root name
        self.osd_roots = dict()
        for osdid, root in zip(self.pg_osds, root_order):
            self.osd_roots[osdid] = root

        # all available placement osds for this crush root
        self.osd_candidates = set()
        for root_name in root_uses.keys():
            for osdid in self.cluster.candidates_for_root(root_name).keys():
                self.osd_candidates.add(osdid)

    def get_osd_candidates(self):
        """
        return all possible candidate OSDs for the PG to relocate.
        """
        return self.osd_candidates

    @staticmethod
    def use_item_type(item_uses, trace, item_type, rule_step):
        """
        given a trace (or a part), walk forward, until a given item type is found.
        increase its use.
        """
        for idx, item in enumerate(trace):
            if item["type_name"] == item_type:
                item_id = item["id"]
                cur_item_uses = item_uses[rule_step].get(item_id, 0)
                cur_item_uses += 1
                item_uses[rule_step][item_id] = cur_item_uses
                return idx
        return None

    def prepare_crush_check(self):
        """
        perform precalculations for moving this pg
        """
        logging.debug(strlazy(lambda: f"prepare crush check for pg {self.pg} currently up={self.pg_osds}"))
        logging.debug(strlazy(lambda: f"rule:\n{pformat(self.rule)}"))

        # example: 4+2 ec -> size=6
        # 4 ssds and 2 hdds
        #
        # root        __________-9____________________________               _-13_
        # room:                                                           -14    -15
        # rack: _____-7_______    _________-8_____      ___-10____        -16    -17
        # host: -1    -2    -3    -4    -5      -6      -11     -12       -18    -19
        # osd: 1 2 | 3 4 | 5 6 | 7 8 | 9 10 | 11 12 | 13 14 | 15 16      17 18 | 19 20
        #        ^     ^         ^     ^                                 ^          ^
        #
        # crush rule with two separate root choices:
        # 0 take root -9
        # 1 choose 2 rack
        # 2 chooseleaf 2 hosts class ssd
        # 3 emit
        #
        # 4 take root -13
        # 5 chooseleaf -2 rack class hdd
        # 6 emit
        #
        # inverse reuse aggregation, starting with 1, for each take.
        # reuses_per_step = [
        #     [4, 2, 2, 1],   # first take
        #     [2, 1, 1],      # second take
        # ]
        #
        # current osds=[2, 4, 7, 9, 17 20]
        #
        # traces from root down to osd:
        #   2: [-9, -7, -1, 2]
        #   4: [-9, -7, -2, 4]
        #   7: [-9, -8, -4, 7]
        #   9: [-9, -8, -5, 9]
        #   17: [-13, -14, -16, -18, 17]
        #   20: [-13, -15, -17, -10, 20]
        #
        # mapping from osdindex to take-index (which take we're in)
        # osd_take_index=[0, 0, 0, 0, 1, 1]
        # mapping from rule step to take-index
        # rule_take_index=[0, 0, 0, 0, 1, 1, 1]
        #
        # reuses_per_step: take-index -> rule_step -> use_counts
        #  [{0: {-9: 4},                          # for rule_step 0
        #    1: {-7: 2, -8: 2},                   # for rule_step 1
        #    2: {-1: 1, -2: 1, -4: 1, -5: 1}}     # for rule_step 2
        #   {0: {-13: 2},                         # for rule_step 4
        #    1: {-16: 1, -17: 1}}]                # for rule_step 5
        #
        # from these use_count, subtract the trace of the replaced osd
        #
        # to eliminate candidates:
        # * replace old_osd with new_osd
        # * find take_index by looking up old_osd in osd_take_index
        # * select reuses from reuses_per_step by take_index
        # * check if new_osd is candidate of the same root as old_osd
        # * get trace to root after replacing with new_osd
        # * check use new counts against reuses_per_step
        #
        # replacement:
        # let's replace osd 2 by:
        # 1 -> get osd trace = [-9 -7, -1, 1]
        #      max_use = reuses_per_step[takeindex=1] = [4, 2, 2, 1]
        #      -9 used 3<4, -7 used 1<2, -1 used 0<1 -> ok
        # 2 -> ...skip, we want to replace it
        # 3 -> -9 used 3<4, -7 used 1<2, -2 used 1<1 -> fail
        # 4 -> keep, not replaced
        # 5 -> -9 used 3<4, -7 used 1<2, -3 used 0<1 -> ok
        # 6 -> -9 used 3<4, -7 used 1<2, -3 used 0<1 -> ok
        # 7 -> keep, not replaced
        # 8 -> -9 used 3<4, -8 used 2<2, -4 used 1<1 -> fail
        # [...]
        # 17 -> not in candidate list of root of 2!
        #
        # replacement candidates for 2: 1, 5, 6, 13 14 15 16
        #
        # let's replace osd 17 by:
        # 1 -> not in candidate list of root of 17!
        # [...]
        # 18 -> ok
        # 19 -> -19 reused too often -> fail

        # take_index -> take_step -> allowed number of bucket reuse
        # e.g. [[6, 2, 1, 1], [3, 1, 1]] for [[root, rack, host, osds], [root, rack, osd]]
        max_reuses = get_max_reuses(self.rule, self.pool_size)
        logging.debug(strlazy(lambda: f"allowed reuses per rule step, starting at root: {pformat(max_reuses)}"))
        # did we encounter an emit?
        emit = False

        # collect trace for each osd.
        # osd -> crush-root-trace
        constraining_traces = dict()

        # how far down the crush hierarchy have we stepped
        # 0 = observe the whole tree
        # each value cuts one layer of the current step's trace
        tree_depth = 0

        # at what rule position is our processing
        rule_step = 0

        # for each rule step, count how often items were used
        # the rule_steps continue counting after several root takes
        # rule_step -> {itemid -> use count}
        item_uses = defaultdict(dict)

        # a rule may have different crush roots
        # this offset slides through the rules' devices
        # for each root.
        rule_device_offset = 0

        # rule_step -> tree_depth to next rule (what tree layer is this rule step)
        # because "choose" steps may skip layers in the crush hierarchy
        # this is then used for the candidate checks:
        # when checking item uses of a new trace,
        # determines which of the trace elements to look at.
        rule_tree_depth = list()

        # only these traces are active due to the current "take"
        take_traces = dict()

        # at which "take" in the crushrule are we?
        take_index = -1
        # what rule step was the last take?
        take_rule_step = -1

        # at which rule step does a take start
        # take_index -> rule_step
        take_rule_steps = dict()
        # take_index -> rule_steps_taken
        take_rule_step_count = dict()

        # record which take a device selection came from
        # osdid -> take_index
        device_take_index = dict()


        # basically analyze how often an item was used
        # due to the crush rule choosing it
        # we go through each (meaningful) step in the rule,
        # see what it selects, and increase the item use counter.
        # this should match the reuses_per_step we determined before.
        #
        # since each step may "skip down" crush tree levels, we have to remember
        # what step used what crush level (so we can later check changes against the same level of a trace)
        for step in self.rule["steps"]:
            if step["op"].startswith("set_"):
                # skip rule configuration steps
                continue

            logging.debug(strlazy(lambda: f"processing crush step {step} with tree_depth={tree_depth}, "
                                          f"rule_step={rule_step}, item_uses={item_uses}"))

            if step["op"] == "take":
                # "take" just defines what crush-subtree we wanna use for choosing devices next.
                current_root_name = step["item_name"]
                tree_depth = 0
                take_index += 1
                take_rule_step = rule_step
                take_rule_steps[take_index] = rule_step
                rule_tree_depth.append(tree_depth)

                # how many devices will be selected in this root?
                root_device_count = max_reuses[take_index][0]
                root_osds = self.pg_osds[rule_device_offset:(rule_device_offset + root_device_count)]

                # generate tracebacks for all osds that ends up in this root.
                # we collect root-traces of all acting osds of the pg we wanna check for.
                for pg_osd in root_osds:
                    trace = trace_crush_root(pg_osd, current_root_name, self.cluster)
                    logging.debug(strlazy(lambda: f"   trace for {pg_osd:4d}: {trace}"))
                    if trace is None or len(trace) < 2:
                        raise Exception(f"no trace found for {pg_osd} in {current_root_name}")
                    constraining_traces[pg_osd] = trace
                    # the root was "used"
                    root_id = trace[0]["id"]
                    root_use = item_uses[rule_step].get(root_id, 0) + 1
                    item_uses[rule_step][root_id] = root_use
                    device_take_index[pg_osd] = take_index

                if not constraining_traces:
                    raise Exception(f"no device traces captured for step {step}")

                # only consider traces for the current choose step
                take_traces = {osdid: constraining_traces[osdid] for osdid in root_osds}

                # we just processed the root depth
                rule_step += 1

            elif step["op"].startswith("choose"):
                # find the new tree_depth by looking how far we need to step for the choosen next bucket
                choose_type = step["type"]

                steps_taken = -1
                for constraining_trace in take_traces.values():
                    # cut each trace at the current tree depth
                    # in that part of the trace, walk forward until we can "choose" the the item we're looking for.
                    # how many steps we took is then returned.
                    used_steps = self.use_item_type(item_uses, constraining_trace[tree_depth:], choose_type, rule_step)
                    if used_steps is None:
                        raise Exception(f"could not find item type {choose_type} "
                                        f"requested by rule step {step}")
                    if steps_taken != -1 and used_steps != steps_taken:
                        raise Exception(f"for trace {constraining_trace} we needed {used_steps} steps "
                                        f"to reach {choose_type}, "
                                        f"but for the previous trace we took {steps_taken} steps!")
                    steps_taken = used_steps

                # how many layers we went down the tree, i.e. how many entries in each trace did we proceed
                tree_depth += steps_taken
                # at what crush hierarchy layer are we now?
                rule_tree_depth.append(tree_depth)
                rule_step += 1

            elif step["op"] == "emit":
                # we may not have reached osd tree level, so we step down further
                # if we have reached it already, we'll take 0 steps.
                steps_taken = -1
                for constraining_trace in take_traces.values():
                    used_steps = self.use_item_type(item_uses, constraining_trace[tree_depth:], "osd", rule_step)
                    if used_steps is None:
                        raise Exception(f"could not find trace steps down to osd"
                                        f"requested by rule step {step}")
                    if steps_taken != -1 and used_steps != steps_taken:
                        raise Exception(f"for trace {constraining_trace} we needed {used_steps} steps "
                                        f"to reach {choose_type}, "
                                        f"but for the previous trace we took {steps_taken} steps!")
                    steps_taken = used_steps

                rule_tree_depth.append(tree_depth + steps_taken)
                take_rule_step_count[take_index] = rule_step - take_rule_steps[take_index] + 1

                rule_step += 1
                emit = True

                # we now emitted this many devices - so we shift what device
                # of the pg we're looking at now.
                rule_device_offset += root_device_count

                # did we enter remember the tree depth for each rule step?
                assert len(rule_tree_depth) == rule_step
                # did we remember how often items were reused in a rule step?
                assert len(item_uses) == rule_step, f"len({item_uses=}) != ({rule_step=})"

                # this take must have as many steps as the max-reuse list has items
                assert take_rule_step_count[take_index] == len(max_reuses[take_index]),\
                        f"{take_rule_step_count[take_index]} != {len(max_reuses[take_index])}"

                # check, for the current take, if collected item uses respect the max_reuses_per_step
                for idx, step_reuses in enumerate(max_reuses[take_index]):
                    for item, uses in item_uses[take_rule_step + idx].items():
                        # uses may be <= since crush rules can emit more osds than the pool size needs
                        if uses > step_reuses:
                            print(f"rule:\n{pformat(self.rule)}")
                            print(f"at take: {take_index}")
                            print(f"reuses: {max_reuses}")
                            print(f"item_uses: {pformat(item_uses)}")
                            print(f"constraining_traces: {pformat(constraining_traces)}")
                            raise Exception(f"during emit, rule take {take_index} at step "
                                            f"{take_rule_step + idx} item {item} was used {uses} > {step_reuses} expected")

            else:
                raise Exception(f"unknown crush operation encountered: {step['op']}")

        if not emit:
            raise Exception("no emit in crush rule processed")

        # we should have processed exactly pool-size many devices
        assert rule_device_offset == self.pool_size
        assert rule_device_offset == len(constraining_traces)
        # for each rule step there should be a tree depth entry
        assert len(rule_tree_depth) == rule_step
        # for each rule step, we should have registered item usages
        assert len(item_uses) == rule_step
        # we should have processed all takes, hence all per-take reuse constraints
        assert (take_index + 1) == len(max_reuses), f"{take_index + 1} != {len(max_reuses)}"

        self.constraining_traces = constraining_traces  # osdid->crush-root-trace
        self.rule_tree_depth = rule_tree_depth  # rule_step->tree_depth
        self.max_reuses = max_reuses  # take_index->rule_step->allowed_item_reuses
        self.item_uses = item_uses  # rule_step->{item->use_count}
        self.device_take_index = device_take_index  # osdid->take_index
        self.take_rule_steps = take_rule_steps  # take_index -> rule_step of take beginning
        self.take_rule_step_count = take_rule_step_count  # take_index -> rule_step_count

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

        # the trace we no longer consider (since we replace the osd)
        old_trace = self.constraining_traces[old_osd]

        # what root name is the old osd in?
        # the new one needs to be under the same root.
        root_name = self.osd_roots[old_osd]

        # which "take" was the old osd in - we need to respect the same "take"
        take_index = self.device_take_index[old_osd]

        # create trace for the replacement candidate
        new_trace = trace_crush_root(new_osd, root_name, self.cluster)

        if new_trace is None:
            # should never be tried as candidate
            raise Exception(f"no trace found for {new_osd} up to {root_name}")

        logging.debug(strlazy(lambda: f"   trace for old osd.{old_osd}: {old_trace}"))
        logging.debug(strlazy(lambda: f"   trace for new osd.{new_osd}: {new_trace}"))

        overuse = False

        # perform the crush steps again, checking for item reusages on each layer.
        step_start = self.take_rule_steps[take_index]
        step_count = self.take_rule_step_count[take_index]
        for idx, tree_stepwidth in enumerate(self.rule_tree_depth[step_start:step_start + step_count]):
            use_max_allowed = self.max_reuses[take_index][idx]

            # as we would remove the old osd trace,
            # the item would no longer be occupied in the new trace
            old_item = old_trace[tree_stepwidth]["id"]
            # this trace now adds to the item uses:
            new_item = new_trace[tree_stepwidth]["id"]

            # how often is new_item used now?
            # if not used at all, it's not in the dict.
            uses = self.item_uses[step_start + idx].get(new_item, 0)

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
            pg_shardsize = self.cluster.get_pg_shardsize(self.pg)

        osds_used = list()
        for osd in self.osd_candidates:
            delta = 0
            if osd == osd_from:
                delta = -pg_shardsize
            elif osd == osd_to:
                delta = pg_shardsize

            if self.cluster.osds[osd]['weight'] == 0 or self.cluster.osds[osd]['crush_weight'] == 0:
                # relative usage of weight 0 is impossible
                continue

            osd_used = self.pg_mappings.get_osd_usage(osd, add_size=delta)
            osds_used.append(osd_used)

        var = statistics.variance(osds_used)
        return var

    def filter_candidate(self, osdid):
        """
        given an an osd id, is it a candidate for moving this pg?
        """
        return osdid in self.osd_candidates


class PGMappings:
    """
    PG mapping simulator
    used to calculate device usage when moving around pgs.
    """

    def __init__(self,
                 cluster,
                 only_crushclasses: Optional[set] = None,
                 only_poolids: Optional[set] = None,
                 osdused_method = OSDUsedMethod.SHARDSUM,
                 osd_from_choice_method = OSDFromChoiceMethod.ALTERNATE,
                 pg_choice_method = PGChoiceMethod.LARGEST,
                 disable_simulation = False):

        # the "real" devices, just used for their "fixed" properties like
        # device size
        self.cluster = cluster

        # how to simulate the osd usage
        self.osdused_method = osdused_method

        # how to select a osd where we then can move a pg from
        self.osd_from_choice_method = osd_from_choice_method

        # how to select pgs to move around
        self.pg_choice_method = pg_choice_method

        # consider only these crushclasses
        self.only_crushclasses = only_crushclasses or cluster.crushclass_osds.keys()

        # which pools are candidates for mapping changes
        self.pool_candidates = set()

        # up state: osdid -> {pg, ...}
        self.osd_pgs_up = defaultdict(set)

        # acting state: osdid -> {pg, ...}
        self.osd_pgs_acting = defaultdict(set)

        # choose the up mapping, since we wanna optimize the "future" cluster
        # up pg mapping: pgid -> [up_osd, ...]
        self.pg_mappings = dict()

        # osdid -> poolid -> count of shards
        # is updated for each move!
        self.osd_pool_up_shard_count = defaultdict(lambda: defaultdict(lambda: 0))

        # osdid -> sum size of all shards on the osd
        self.osd_shardsize_sum = defaultdict(lambda: 0)

        for pg, pginfo in self.cluster.pgs.items():
            up_osds = pginfo["up"]
            self.pg_mappings[pg] = list(up_osds)

            for osdid in up_osds:
                self.osd_pgs_up[osdid].add(pg)
                self.osd_pool_up_shard_count[osdid][pool_from_pg(pg)] += 1
                self.osd_shardsize_sum[osdid] += self.cluster.get_pg_shardsize(pg)

            acting_osds = pginfo["acting"]
            for osdid in acting_osds:
                self.osd_pgs_acting[osdid].add(pg)

        # don't prepare as much if we don't want to simulate movements.
        self.disable_simulation = disable_simulation
        if disable_simulation:
            return

        # pg->[(osd_from, osd_to), ...]
        self.remaps = dict()
        # {osd we used to move from}
        self.from_osds_used = set()

        # collect crushclasses from all enabled pools
        self.enabled_crushclasses = cluster.crushclass_osds.keys()
        self.pool_candidates = cluster.pools.keys()

        # if we have pool id restrictions
        if only_poolids:
            # only keep crushclasses used in the given pools
            self.enabled_crushclasses = set()
            for poolid in only_poolids:
                pool = cluster.pools[poolid]
                pool_size = pool['size']
                pool_crushrule = cluster.crushrules[pool['crush_rule']]
                for root_name in root_uses_from_rule(pool_crushrule, pool_size)[0].keys():
                    for osdid in cluster.candidates_for_root(root_name).keys():
                        self.enabled_crushclasses.add(cluster.osd_crushclass[osdid])

            self.pool_candidates = only_poolids

        # if we have a crushclass restriction, figure out which pools can be affected.
        # this further reduces the pool candidates
        if only_crushclasses:
            pool_crush_candidates = set()

            # figure out which pools are affected by these crushclasses
            for poolid, poolprops in cluster.pools.items():
                rootweights = rootweights_from_rule(cluster.crushrules[poolprops['crush_rule']], poolprops["size"])

                # crush root names used in that pool
                crushroots = rootweights.keys()
                for crushroot in rootweights.keys():
                    # cut deviceclass from crush root name -> default~hdd -> get class hdd
                    classsplit = crushroot.split('~')
                    if len(classsplit) == 2:
                        rootname, deviceclass = classsplit
                        if deviceclass in only_crushclasses:
                            pool_crush_candidates.add(poolid)
                    else:
                        # no explicit device class given in crush rule
                        # -> check if any of this root's candidate's osd crushclass matches the enabled ones
                        for osdid in cluster.candidates_for_root(crushroot):
                            if cluster.osd_crushclass[osdid] in only_crushclasses:
                                pool_crush_candidates.add(poolid)
                                break

            # filter the pool candidates by the crush constraint.
            self.pool_candidates &= pool_crush_candidates
            self.enabled_crushclasses &= only_crushclasses

        # collect all possible osd candidates for discovered crushclasses remove weight=0
        # which we can consider for moving from/to
        self.osd_candidates = set()
        for crushclass in self.enabled_crushclasses:
            self.osd_candidates.update(self.cluster.get_crushclass_osds(crushclass, skip_zeroweight=True))

        # osdid -> used kb, based on the osd-level utilization report
        # so we can simulate movements.
        # this only contains the candidates active due to their crushclass.
        self.osd_utilizations = dict()
        for osdid in self.osd_candidates:
            osd = self.cluster.osds[osdid]
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
                shardsize = self.cluster.get_pg_shardsize(pg)
                pginfo = self.cluster.pgs[pg]

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

                    # get the real current remaps active in the cluster
                    moves = self.cluster.get_remaps(pginfo)

                    # TODO in pginfo there's shard status, but that doesn't seem to contain
                    # sensible content?

                    for osds_from, osds_to in moves:
                        # -1 means in from -> move is degraded for EC
                        # for replica, len(from) < len(to) = degraded
                        if self.cluster.pg_is_ec(pg):
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
                osd_fs_used -= self.cluster.get_pg_shardsize(pg)

            self.osd_utilizations[osdid] = osd_fs_used
            if False:
                logging.debug(strlazy(lambda: (
                    f"estimated {'osd.%s' % osdid: <8} weight={osd['weight']:.4f} "
                    f"#acting={len(self.osd_pgs_acting[osdid]):<3} "
                    f"acting={pformatsize(osd['device_used'], 3)}={0 if osd['device_size'] == 0 else ((100 * osd['device_used']) / osd['device_size']):.03f}% "
                    f"#up={len(self.osd_pgs_up[osdid]):<3} "
                    f"up={pformatsize(osd_fs_used, 3)}={0 if osd['device_size'] == 0 else ((100 * osd_fs_used) / osd['device_size']):.03f}% "
                    f"size={pformatsize(osd['device_size'], 3)}")))

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
        shard_size = self.cluster.get_pg_shardsize(pg)
        self.osd_utilizations[osd_from] -= shard_size
        self.osd_utilizations[osd_to] += shard_size

        self.osd_shardsize_sum[osd_from] -= shard_size
        self.osd_shardsize_sum[osd_to] += shard_size

        poolid = pool_from_pg(pg)
        self.osd_pool_up_shard_count[osd_from][poolid] -= 1
        self.osd_pool_up_shard_count[osd_to][poolid] += 1

        self.get_osd_usage.cache_clear()
        self.get_osd_usage_size.cache_clear()
        self.get_osd_space_allocated.cache_clear()
        self.get_pool_max_avail_pgs_limit.cache_clear()
        self.get_pool_max_avail_weight.cache_clear()

        # insert a new mapping tracker
        self.remaps.setdefault(pg, []).append((osd_from, osd_to))

        self.from_osds_used.add(osd_from)

    def get_enabled_crushclasses(self):
        """
        osds of which crushclasses are we gonna touch?
        """
        return self.enabled_crushclasses

    def get_potentially_affected_pools(self):
        """
        returns set(poolid) for pools that may be adjusted through changes in the mappings
        """
        return self.pool_candidates

    def get_osd_usages(self, add_size=0):
        """
        calculate {osdid -> usage percent}
        """
        return {osdid: self.get_osd_usage(osdid, add_size) for osdid in self.osd_candidates}

    def get_osd_usages_asc(self):
        """
        return [(osdid, usage in percent)] sorted ascendingly by usage
        """
        # from this mapping, calculate the weighted osd size,
        return list(sorted(self.get_osd_usages().items(), key=lambda osd: osd[1]))

    def get_mapping(self, pg):
        return self.pg_mappings[pg]

    def get_osd_pgs_up(self, osdid):
        return self.osd_pgs_up[osdid]

    def get_osd_pgs_acting(self, osdid):
        return self.osd_pgs_acting[osdid]

    def get_pg_move_candidates(self, osdid):
        return PGCandidates(self, osdid,
                            pool_candidates=self.pool_candidates,
                            pg_choice_method=self.pg_choice_method)

    def get_osd_from_candidates(self):
        """
        generate source candidates to move a pg from.

        method: fullest
        use the relatively fullest osd.

        method: limiting
        first, use the limiting osds from predicted pool usage,
        then continue with the relatively fullest osds.

        method: limit_hybrid:
        pick from limiting, then fullest, then limiting, etc.
        """

        alternate = False
        pool_limit = False
        fullest_osd = False

        if self.osd_from_choice_method == OSDFromChoiceMethod.LIMITING:
            pool_limit = True
            fullest_osd = True
        elif self.osd_from_choice_method == OSDFromChoiceMethod.FULLEST:
            fullest_osd = True
        elif self.osd_from_choice_method == OSDFromChoiceMethod.ALTERNATE:
            alternate = True
            pool_limit = True
            fullest_osd = True
        else:
            raise Exception(f"unknown OSDFromChoiceMethod: {self.osd_from_choice_method}")

        emitted = set()

        def limiting_osds():
            # [(limiting_osdid, usage), ]
            limiting_osds = list()
            for poolid in self.pool_candidates:
                # all limiting osds
                osdid = self.get_pool_max_avail_pgs_limit(poolid)[1]
                limiting_osds.append((osdid, self.get_osd_usage(osdid)))

            for osdid, usage in sorted(limiting_osds, key=lambda x: x[1], reverse=True):
                yield osdid, usage

        def fullest_osds():
            for osdid, usage in sorted(self.get_osd_usages().items(),
                                       key=lambda osd: osd[1],
                                       reverse=True):
                yield osdid, usage

        iterators = list()
        if pool_limit:
            iterators.append(limiting_osds())

        if fullest_osd:
            iterators.append(fullest_osds())

        if alternate:
            def alternator(iterators):
                for value in zip_longest(*iterators):
                    for elem in value:
                        if elem is not None:
                            yield elem
            iterators = [alternator(iterators)]

        for osdid, usage in chain(*iterators):
            if osdid in emitted:
                continue
            emitted.add(osdid)
            yield osdid, usage


    def get_osd_target_candidates(self, move_pg: Optional[str] = None):
        """
        generate target candidates to move a pg to.

        method: emptiest
        use the relatively emptiest osd.
        """

        move_pg_shardsize = 0
        if move_pg:
            move_pg_shardsize = self.cluster.get_pg_shardsize(move_pg)

        # emit empty first
        for osdid, usage in sorted(self.get_osd_usages(add_size=move_pg_shardsize).items(),
                                   key=lambda osd: osd[1],
                                   reverse=False):
            yield osdid, usage

    def get_osd_shardsize_sum(self, osdid):
        """
        calculate the osd usage by summing all the mapped (up) PGs shardsizes
        -> we can calculate a future size
        """
        return self.osd_shardsize_sum[osdid]

    @lru_cache(maxsize=2**18)
    def get_osd_space_allocated(self, osdid):
        """
        return the osd usage reported by the osd, but with added shardsizes
        when pg movements were applied.
        """
        return self.osd_utilizations[osdid]

    @lru_cache(maxsize=2**18)
    def get_osd_usage_size(self, osdid, add_size=0):
        """
        returns the occupied space on an osd.

        can be calculated by two variants:
        - by estimating the placed shards.
          - either by summing up all shards
          - or by subtracting moved-away shards from the allocated space
        - add to the used data amount with add_size.
        """
        if self.disable_simulation:
            # basically "delta", but without pending pg moves
            used = self.cluster.osds[osdid]['device_used']
        elif self.osdused_method == OSDUsedMethod.DELTA:
            used = self.get_osd_space_allocated(osdid)
        elif self.osdused_method == OSDUsedMethod.SHARDSUM:
            used = self.get_osd_shardsize_sum(osdid)
        else:
            raise Exception(f"unknown osd usage estimator: {self.osdused_method!r}")

        used += add_size

        return used

    @lru_cache(maxsize=2**18)
    def get_osd_usage(self, osdid, add_size=0):
        """
        returns the occupied OSD space in percent.
        """
        osd_size = self.cluster.get_osd_size(osdid)

        if osd_size == 0:
            raise Exception(f"getting relative usage of osd.{osdid} which has size 0 impossible")

        used = self.get_osd_usage_size(osdid, add_size)

        # make it relative
        used /= osd_size
        used *= 100
        # logging.debug(f"{osdid} {pformatsize(used)}/{pformatsize(osd_size)} = {used/osd_size * 100:.2f}%")

        return used

    def get_cluster_variance(self):
        """
        get {crushclass -> variance} for given mapping
        """
        variances = dict()
        for crushclass in self.get_enabled_crushclasses():
            osd_usages = list()
            for osdid in self.cluster.get_crushclass_osds(crushclass, skip_zeroweight=True):
                osd_usages.append(self.get_osd_usage(osdid))

            if len(osd_usages) > 1:
                class_variance = statistics.variance(osd_usages)
            elif osd_usages:
                class_variance = osd_usages[0]
            else:
                raise Exception("no osds in crushclass, but variance requested")
            variances[crushclass] = class_variance

        return variances

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
        current_upmaps = self.cluster.upmap_items.get(pgid, [])

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
                raise Exception(f"somewhere something went wrong, we map {pgid} from osd.{new_from} to osd.{new_to}")

        return resulting_upmaps

    @lru_cache(maxsize=2**16)
    def get_pool_max_avail_weight(self, poolid):
        """
        given a pool id, predict how much space is available with the current mapping.
        similar how `ceph df` calculates available pool size

        approach using just crush weights.
        this calculates maximum available space if the pool hat infinity many pgs.
        """

        pool = self.cluster.pools[poolid]
        blowup_rate = pool['blowup_rate']
        pool_size = pool['size']

        # scale by objects_there/objects_expected
        if pool['num_object_copies'] > 0:
            blowup_rate *= (pool['num_object_copies'] - pool['num_objects_degraded']) / pool['num_object_copies']

        # cf. PGMap::get_rule_avail
        # {osdid -> osd_weight_probability=[selectionweight/allcandidateweights]}
        osdid_candidates = self.cluster.candidates_for_pool(poolid)

        # how much space is available in a pool?
        # this is determined by the "fullest" osd:
        # we try for each osd how much it can take,
        # given its selection probabiliy (due to its weight)
        min_avail = -1
        for osdid, osdweight in osdid_candidates.items():
            # since we will adjust weight below, we need the raw device size
            device_size = self.cluster.osd_devsize[osdid]
            # shrink the device size by configured cluster full_ratio
            device_size *= self.cluster.full_ratio

            used_size = self.get_osd_usage_size(osdid)
            usable = device_size - used_size
            # how much space will be occupied on this osd by the pool?
            # for that, take the amount of current osd weight into account,
            # relative to whole pool weight possible for all osds.
            # how much of the whole distribution will be "this" osd.
            weighted_usable = usable / (osdweight * blowup_rate)

            #print(
            #    f"usable on {osdid:>4}: "
            #    f"size={pformatsize(device_size):>5} "
            #    f"used={pformatsize(used_size):>8} "
            #    f"usable={pformatsize(usable):>7} "
            #    f"blowup={blowup_rate:>7} "
            #    f"weight={osdweight:>7} "
            #    f"weighted={pformatsize(weighted_usable, 2):>7} "
            #)

            if min_avail < 0 or weighted_usable < min_avail:
                min_avail = weighted_usable
        pool_avail = min_avail

        return pool_avail

    @lru_cache(maxsize=2**16)
    def get_pool_max_avail_pgs_limit(self, poolid):
        """
        given a pool id, predict how much space is available with the current mapping.

        uses the current pg distribution on an osd and hence
        free_osd_space * (pool_pg_num / ((count(pool_pgs) on this osd) * blowup_rate))

        this calculates maximum for the current pg placement.

        returns (max_avail, limiting_osd)
        """

        pool = self.cluster.pools[poolid]
        pool_pg_num = pool['pg_num']
        pool_size = pool['size']
        blowup_rate = pool['blowup_rate']
        num_shards = pool_pg_num * pool_size

        osdid_candidates = self.cluster.candidates_for_pool(poolid).keys()

        # how much space is available in a pool?
        # we try for each osd how much it can take,
        # given its selection probabiliy (due to pg distributions)
        min_avail = -1
        limiting_osd = -1

        for osdid in osdid_candidates:
            # raw device size as reported by osd
            device_size = self.cluster.osd_devsize[osdid]
            # shrink the device size by configured cluster full_ratio
            device_size *= self.cluster.full_ratio

            used_size = self.get_osd_usage_size(osdid)
            usable = device_size - used_size

            # how much space will be occupied on this osd by the pool?
            # for that, take the amount of current osd weight into account,
            # relative to whole pool weight possible for all osds.
            # how much of the whole distribution will be "this" osd.
            pool_pg_shards_on_osd = self.osd_pool_up_shard_count[osdid][poolid]
            if pool_pg_shards_on_osd == 0:
                # no pg of this pool is on this osd.
                # so this osd won't be filled up by this pool.
                continue

            # this is the "real" size prediction of an osd:
            # (usable_on_osd * pool_usable_rate) / osd_probability_for_getting_data_of_pool

            placement_probability = pool_pg_shards_on_osd / num_shards
            predicted_usable = usable / (placement_probability * blowup_rate)

            if min_avail < 0 or predicted_usable < min_avail:
                min_avail = predicted_usable
                limiting_osd = osdid

            if False:
                print(
                    f"usable on {osdid:>4}: "
                    f"size={pformatsize(device_size):>5} "
                    f"used={pformatsize(used_size):>8} "
                    f"usable={pformatsize(usable):>7} "
                    f"raw_blowup={blowup_rate:>7} "
                    f"on_osd={pool_pg_shards_on_osd:>4} "
                    f"probability={placement_probability:>7} "
                    f"weighted={pformatsize(predicted_usable, 2):>7} "
                )


        if min_avail < 0:
            raise Exception(f"no pg of {poolid=} mapped on any osd to use space")

        return min_avail, limiting_osd

    def get_pool_max_avail_pgs(self, poolid):
        return self.get_pool_max_avail_pgs_limit(poolid)[0]

    def get_pool_max_avail(self, poolid):
        """
        calculate how many bytes can be stored in this pool at current
        pg distribution.
        """

        # good when we want to know if we had n->\inf many pgs
        #byweight = self.get_pool_max_avail_weight(poolid)
        # good when we want to predict about the current pg placement
        bypgs = self.get_pool_max_avail_pgs(poolid)
        if False:
            print(
                f"on {poolid}: original={pformatsize(self.cluster.pools[poolid]['store_avail'])} "
                f"weight={pformatsize(byweight, 2)} "
                f"pg={pformatsize(bypgs, 2)} "
            )
        return bypgs


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

    def __init__(self, pg_mappings, osd, pool_candidates, pg_choice_method):
        up_pgs = pg_mappings.get_osd_pgs_up(osd)
        acting_pgs = pg_mappings.get_osd_pgs_acting(osd)
        remapped_pgs = up_pgs - acting_pgs

        self.cluster = pg_mappings.cluster

        self.pg_candidates = list()
        self.pg_properties = dict()

        # we can move up pgs away - they are on the osd or planned to be on it.
        for pgid in up_pgs:
            pg_pool = pool_from_pg(pgid)
            if pool_candidates and pg_pool not in pool_candidates:
                continue

            self.pg_candidates.append(pgid)

            self.pg_properties[pgid] = PGShardProps(
                size=self.cluster.get_pg_shardsize(pgid),
                remapped=(pgid in remapped_pgs),   # the shard is not yet fully moved
                upmap_count=len(pg_mappings.get_pg_upmap(pgid))
            )

        # best candidate first
        pg_candidates_desc = list(sorted(self.pg_candidates,
                                         key=lambda pg: self.pg_properties[pg]))

        # reorder potential pg_candidates by configurable approaches
        pg_walk_anchor = None

        # TODO: move these choice methods also into the PGShardProps comparison function
        if pg_choice_method == PGChoiceMethod.AUTO:
            # choose the PG choice method based on
            # the utilization differences of the fullest and emptiest candidate OSD.

            # if we could move there, most balance would be created.
            # use this to guess the shard size to move.
            best_target_osd, best_target_osd_usage = next(pg_mappings.get_osd_target_candidates())

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

        elif pg_choice_method == PGChoiceMethod.LARGEST:
            self.pg_candidates = pg_candidates_desc

        elif pg_choice_method == PGChoiceMethod.MEDIAN:
            # here, we decide to move not the largest/smallest pg, but rather the median one.
            # order PGs around the median-sized one
            # [5, 4, 3, 2, 1] => [3, 4, 2, 5, 1]
            pg_candidates_desc_sizes = [self.pg_properties[pg].size for pg in pg_candidates_desc]
            pg_candidates_median = statistics.median_low(pg_candidates_desc_sizes)
            pg_walk_anchor = pg_candidates_desc_sizes.index(pg_candidates_median)

        else:
            raise Exception(f'unhandled shard choice {pg_choice_method!r}')

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

    def get_properties(self, pgid):
        return self.pg_properties[pgid]

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


def balance(args, cluster):
    logging.info("running pg balancer")

    # this is basically my approach to OSDMap::calc_pg_upmaps
    # and a CrushWrapper::try_remap_rule python-implementation

    if args.osdused == "delta":
        osdused_method = OSDUsedMethod.DELTA
    elif args.osdused == "shardsum":
        osdused_method = OSDUsedMethod.SHARDSUM
    else:
        raise Exception(f"unknown osd usage rate method {args.osdused!r}")

    if args.osdfrom == "fullest":
        osdfrom_method = OSDFromChoiceMethod.FULLEST
    elif args.osdfrom == "limiting":
        osdfrom_method = OSDFromChoiceMethod.LIMITING
    elif args.osdfrom == "alternate":
        osdfrom_method = OSDFromChoiceMethod.ALTERNATE
    else:
        raise Exception(f"unknown osd usage rate method {args.osdused!r}")

    if args.pg_choice == "largest":
        pg_choice_method = PGChoiceMethod.LARGEST
    elif args.pg_choice == "median":
        pg_choice_method = PGChoiceMethod.MEDIAN
    elif args.pg_choice == "auto":
        pg_choice_method = PGChoiceMethod.AUTO
    else:
        raise Exception(f"unknown osd usage rate method {args.pg_choice!r}")

    only_crushclasses = None
    if args.only_crushclass and not (args.only_pool or args.only_poolid):
        only_crushclasses = {cls.strip() for cls in args.only_crushclass.split(",") if cls.strip()}
        logging.info(f"only considering crushclasses {only_crushclasses}")

    only_poolids = None
    if args.only_poolid:
        only_poolids = {int(pool) for pool in args.only_poolid.split(",") if pool.strip()}

    if args.only_pool:
        only_poolids = {int(cluster.poolnames[pool.strip()]) for pool in args.only_pool.split(",") if pool.strip()}

    if only_poolids:
        logging.info(f"only considering pools {only_poolids}")
        if args.only_crushclass:
            logging.info(f"  ignoring the only-crushclass option since explicit pools were given.")


    # we'll do all the optimizations in this mapping state
    pg_mappings = PGMappings(cluster,
                             only_crushclasses=only_crushclasses,
                             only_poolids=only_poolids,
                             osdused_method=osdused_method,
                             osd_from_choice_method=osdfrom_method,
                             pg_choice_method=pg_choice_method)

    # poolids for all that can be balanced due to their candidates osds having their contents moved around
    pools_affected_by_balance = pg_mappings.get_potentially_affected_pools()

    # to restrict source osds
    source_osds = None
    if args.source_osds:
        source_osds = [int(osdid) for osdid in args.source_osds.split(',')]

    # number of found remaps
    found_remap_count = 0
    # size of found remaps
    found_remap_size_sum = 0
    force_finish = False

    logging.info("current OSD fill rate per crushclasses:")
    for crushclass in pg_mappings.get_enabled_crushclasses():
        usages = list()
        for osdid in cluster.get_crushclass_osds(crushclass, skip_zeroweight=True):
            usages.append(pg_mappings.get_osd_usage(osdid))

        # fillaverage: calculated current osd usage
        # crushclassusage: used percent of all space provided by this crushclass
        fillaverage = statistics.mean(usages)
        fillmedian = statistics.median(usages)
        crushclass_usage = cluster.crushclasses_usage[crushclass]
        logging.info(f"  {crushclass}: average={fillaverage:.2f}%, median={fillmedian:.2f}%, without_placement_constraints={crushclass_usage:.2f}%")

    init_cluster_variance = pg_mappings.get_cluster_variance()
    cluster_variance = init_cluster_variance

    logging.info("cluster variance for crushclasses:")
    for crushclass, variance in init_cluster_variance.items():
        logging.info(f"  {crushclass}: {variance:.3f}")

    # TODO: track these min-max values per crushclass
    osd_usages_asc = pg_mappings.get_osd_usages_asc()
    init_osd_min, init_osd_min_used = osd_usages_asc[0]
    init_osd_max, init_osd_max_used = osd_usages_asc[-1]
    osd_min = init_osd_min
    osd_min_used = init_osd_min_used
    osd_max = init_osd_max
    osd_max_used = init_osd_max_used
    logging.info(f"min osd.{init_osd_min} {init_osd_min_used:.3f}%")
    logging.info(f"max osd.{init_osd_max} {init_osd_max_used:.3f}%")

    init_pool_avail = dict()

    logging.debug("currently usable space, caused by osd")
    logging.debug(strlazy(lambda: f"  {'pool'.ljust(cluster.max_poolname_len)} {'size': >8} {'osd': >6}"))
    for poolid in pools_affected_by_balance:
        poolname = cluster.pools[poolid]['name'].ljust(cluster.max_poolname_len)
        avail, limit_osd = pg_mappings.get_pool_max_avail_pgs_limit(poolid)
        init_pool_avail[poolid] = (avail, limit_osd)
        logging.debug(strlazy(lambda: f"  {poolname} {pformatsize(avail, 2): >8} {limit_osd: >6}"))

    while True:
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
        for osd_from, osd_from_used_percent in pg_mappings.get_osd_from_candidates():
            if found_remap or force_finish:
                break

            # filter only-allowed source osds
            if source_osds is not None:
                if osd_from not in source_osds:
                    continue

            source_attempts += 1

            if source_attempts > args.max_move_attempts:
                logging.info(f"couldn't empty osd.{last_attempt}, so we're done. "
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

            from_osd_pg_count = pg_mappings.osd_pool_up_shard_count[osd_from]

            from_osd_satisfied_pools = set()

            # now try to move the candidates to their possible destination
            # TODO: instead of having all candidates,
            #       always get the "next" one from a "smart" iterator
            #       there we can also feed in the current osd_usages_asc
            #       in order to "optimize" candidates by "anticipating" "target" "osds" "."
            #       even better, we can integrate the PGMoveChecker directly, and thus calculate
            #       target OSD candidates _within_ the pg candidate generation.
            for move_pg_idx, move_pg in enumerate(pg_candidates.get_candidates()):
                if found_remap or force_finish:
                    break

                pg_pool = pool_from_pg(move_pg)

                if pg_pool in from_osd_satisfied_pools:
                    logging.debug("SKIP pg %s from osd.%s since pool=%s-pgs are already below expected", move_pg, osd_from, pg_pool)
                    continue

                if pg_pool in unsuccessful_pools:
                    logging.debug("SKIP pg %s from osd.%s since pool (%s) can't be balanced more", move_pg, osd_from, pg_pool)
                    continue

                move_pg_shardsize = pg_candidates.get_size(move_pg)
                logging.debug("TRY-0 moving pg %s (%s/%s) with %s from osd.%s", move_pg, move_pg_idx+1, len(pg_candidates), pformatsize(move_pg_shardsize), osd_from)

                try_pg_move = PGMoveChecker(pg_mappings, move_pg)
                pool_pg_shard_count_ideal = cluster.pool_pg_shard_count_ideal(pg_pool, try_pg_move.get_osd_candidates())
                from_osd_pg_count_ideal = pool_pg_shard_count_ideal * cluster.get_osd_size(osd_from)

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

                for osd_to, osd_to_usage in pg_mappings.get_osd_target_candidates(move_pg):
                    logging.debug("TRY-1 move %s osd.%s => osd.%s", move_pg, osd_from, osd_to)

                    if not try_pg_move.filter_candidate(osd_to):
                        # e.g. crushclass doesn't match.
                        continue

                    if osd_to == osd_from:
                        logging.debug(" BAD move to source OSD makes no sense")
                        continue

                    # in order to not fight the regular balancer, don't move the PG to a disk
                    # where the weighted pg count of this pool is already good
                    # otherwise the balancer will move a pg on the osd_from of this pool somewhere else
                    # i.e. don't be the +1
                    to_osd_pg_count = pg_mappings.osd_pool_up_shard_count[osd_to]
                    to_osd_pg_count_ideal = pool_pg_shard_count_ideal * cluster.get_osd_size(osd_to)
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

                    # for logging below
                    prev_pg_mapping = cluster.pg_osds_up[move_pg]
                    new_pg_mapping = list()
                    for idx, osdid in enumerate(prev_pg_mapping):
                        if osdid == osd_from:
                            osdid = osd_to
                            new_mapping_pos = idx
                        new_pg_mapping.append(osdid)

                    # record the mapping
                    pg_mappings.apply_remap(move_pg, osd_from, osd_to)

                    # what's the min/max osd now?
                    osd_usages_asc = pg_mappings.get_osd_usages_asc()
                    osd_min, osd_min_used = osd_usages_asc[0]
                    osd_max, osd_max_used = osd_usages_asc[-1]
                    cluster_variance = pg_mappings.get_cluster_variance()

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

    # poolid -> (prev, new, gained space)
    space_won = dict()
    for poolid in pools_affected_by_balance:
        prev_avail, prev_limit_osd = init_pool_avail[poolid]
        new_avail = pg_mappings.get_pool_max_avail_pgs(poolid)
        gain = new_avail - prev_avail
        if gain > 0:
            space_won[poolid] = (prev_avail, new_avail, gain)
        else:
            limit_used = prev_limit_osd in pg_mappings.from_osds_used
            if limit_used:
                logging.info(f"gain == 0 even though we moved from {prev_limit_osd}")

    if space_won:
        logging.info(strlazy(lambda: f"new usable space:"))
        poolnameheader = 'pool name'.ljust(cluster.max_poolname_len)
        logging.info(strlazy(lambda: f"{poolnameheader} {'previous': >8}   {'new': >8}   {'gain': >8}"))

        for poolid, (prev_avail, new_avail, gain) in space_won.items():
            poolname = cluster.pools[poolid]['name'].ljust(cluster.max_poolname_len)
            logging.info(strlazy(lambda: (f"  {poolname} {pformatsize(prev_avail, 2): >8} "
                                          f"-> {pformatsize(new_avail, 2): >8} => {pformatsize(gain, 2): >8}")))
    else:
        logging.info(strlazy(lambda: f"done. this probably gain any space since no limiting osd is affected"))

    # print what we have been waiting for :)
    for pg, upmaps in pg_mappings.get_upmaps().items():
        if upmaps:
            upmap_str = " ".join([f"{osd_from} {osd_to}" for (osd_from, osd_to) in upmaps])
            print(f"ceph osd pg-upmap-items {pg} {upmap_str}")
        else:
            print(f"ceph osd rm-pg-upmap-items {pg}")


def show(args, cluster):
    if args.format == 'plain':

        maxpoolnamelen = cluster.max_poolname_len

        maxcrushclasslen = 0
        for crush_class in cluster.crushclass_osds.keys():
            if len(crush_class) > maxcrushclasslen:
                maxcrushclasslen = len(crush_class)

        if args.show_max_avail:
            maxavail_hdr = f" {'maxavail': >8}"
        else:
            maxavail_hdr = ""

        poolname = 'name'.ljust(maxpoolnamelen)
        print()
        print(f"{'poolid': >6} {poolname} {'type': <7} {'size': >5} {'min': >3} {'pg_num': >6} {'stored': >7} {'used': >7} {'avail': >7}{maxavail_hdr} {'shrdsize': >8} crush")

        # default, sort by pool id
        sort_func = lambda x: x[0]

        if args.sort_shardsize:
            sort_func = lambda x: x[1]['pg_shard_size_avg']

        stored_sum = 0
        stored_sum_class = defaultdict(int)
        used_sum = 0
        used_sum_class = defaultdict(int)

        for poolid, poolprops in sorted(cluster.pools.items(), key=sort_func):
            poolname = poolprops['name'].ljust(maxpoolnamelen)
            repl_type = poolprops['repl_type']
            if repl_type == "ec":
                profile = cluster.ec_profiles[poolprops['erasure_code_profile']]
                repl_type = f"ec{profile['data_chunks']}+{profile['coding_chunks']}"

            crushruleid = poolprops['crush_rule']
            crushrule = cluster.crushrules[crushruleid]
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

            if args.show_max_avail or not args.original_avail_prediction:
                mappings = PGMappings(cluster, disable_simulation=True)

            # predicted pool free space - either our or ceph's original size prediction
            if args.original_avail_prediction:
                avail = pformatsize(poolprops['store_avail'])
            else:
                avail = pformatsize(mappings.get_pool_max_avail_pgs(poolid))

            if args.show_max_avail:
                maxavail = pformatsize(mappings.get_pool_max_avail_weight(poolid))
                maxavail = f" {maxavail: >8}"
            else:
                maxavail = ""

            shard_size = pformatsize(poolprops['pg_shard_size_avg'])
            rootweights_pp = ",".join(rootweights_ppl)
            print(f"{poolid: >6} {poolname} {repl_type: <7} {size: >5} {min_size: >3} {pg_num: >6} {stored: >7} {used: >7} {avail: >7}{maxavail} {shard_size: >8} {crushruleid}:{crushrulename} {rootweights_pp: >12}")

        for crushroot in stored_sum_class.keys():
            crushclass_usage = ""
            crush_tree_class = crushroot.split("~")
            if len(crush_tree_class) >= 2:
                crush_class = crush_tree_class[1]
                crushclass_usage = f"{cluster.crushclasses_usage[crush_class]:.3f}%"

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

            for osdid, props in cluster.osds.items():
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
                        used += cluster.get_pg_shardsize(pg)

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
            'pools': cluster.pools,
            'osds': osds,
        }

        print(json.dumps(ret))


def showremapped(args, cluster):
    # pgid -> move information
    pg_move_status = dict()

    for pg, pginfo in cluster.pgs.items():
        pgstate = pginfo["state"].split("+")
        if "remapped" in pgstate:

            moves = list()
            osd_move_count = 0
            for osds_from, osds_to in cluster.get_remaps(pginfo):
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

            pg_move_size = cluster.get_pg_shardsize(pg)
            pg_move_size_pp = pformatsize(pg_move_size)

            pg_move_status[pg] = {
                "state": state,
                "objs_total": objs_total,
                "objs_pending": objs_total - objs_to_restore,
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
                osd_d_size = cluster.osds[osdid]['device_size']
                osd_d_size_pp = pformatsize(osd_d_size, 2)

                osd_d_used = cluster.osds[osdid]['device_used']
                osd_d_used_pp = pformatsize(osd_d_used, 2)

                osd_c_size = cluster.osds[osdid]['crush_weight'] * cluster.osds[osdid]['weight']
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

            print(f"{osdname}: {cluster.osds[osdid]['host_name']}  =>{sum_to} {sum_data_to_pp} <={sum_from} {sum_data_from_pp}"
                  f" (\N{Greek Capital Letter Delta}{sum_data_delta_pp}) {fullness}")

            for pg, to_osd in actions["to"].items():
                pgstatus = pg_move_status[pg]
                print(f"  ->{pg: <6} {pgstatus['state']} {pgstatus['sizepp']: >6} {osdid: >4}->{to_osd: <4} {pgstatus['objs_pending']} of {pgstatus['objs_total']}, {pgstatus['progress']:.1f}%")
            for pg, from_osd in actions["from"].items():
                pgstatus = pg_move_status[pg]
                print(f"  <-{pg: <6} {pgstatus['state']} {pgstatus['sizepp']: >6} {osdid: >4}<-{from_osd: <4} {pgstatus['objs_pending']} of {pgstatus['objs_total']}, {pgstatus['progress']:.1f}%")
            print()

    else:
        for pg, pgmoveinfo in pg_move_status.items():
            state = pgmoveinfo['state']
            move_size_pp = pgmoveinfo['sizepp']
            objs_total = pgmoveinfo['objs_total']
            objs_pending = pgmoveinfo['objs_pending']
            progress = pgmoveinfo['progress']
            move_actions = ';'.join(pgmoveinfo['moves'])
            print(f"pg {pg: <6} {state} {move_size_pp: >6}: {objs_pending} of {objs_total}, {progress:.1f}%, {move_actions}")


def poolosddiff(args, cluster):
    if args.pgstate == "up":
        pool_osds = cluster.pool_osds_up
    elif args.pgstate == "acting":
        pool_osds = cluster.pool_osds_acting
    else:
        raise Exception("unknown pgstate {args.pgstate!r}")

    p1_id = cluster.poolnames[args.pool1]
    p2_id = cluster.poolnames[args.pool2]

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


def repairstats(args, cluster):
    stats_sum = self.state["pg_dump"]["pg_map"]["osd_stats_sum"]
    print(f"repaired reads globally: {stats_sum['num_shards_repaired']}")
    for osdid, osdinfos in osds.items():
        repairs = osdinfos["stats"]["num_shards_repaired"]
        if repairs != 0:
            print(f"repaired on {osdid:>6}: {repairs}")


def main():
    args = parse_args()

    log_setup(args.verbose - args.quiet)

    if args.mode == 'gather':
        if args.statefile:
            raise Exception("in gather mode, a state file can't be used as source")
        state = ClusterState()
        state.dump(args.output_file)

    else:
        if args.osdsize == "device":
            osdsize_method = OSDSizeMethod.DEVICE
        elif args.osdsize == "weighted":
            osdsize_method = OSDSizeMethod.WEIGHTED
        elif args.osdsize == "crush":
            osdsize_method = OSDSizeMethod.CRUSH
        else:
            raise Exception(f"unknown osd weight method {args.osdsize!r}")

        state = ClusterState(args.state, osdsize_method=osdsize_method)
        state.preprocess()

        if args.mode == 'balance':
            balance(args, state)

            #import cProfile
            #from pstats import SortKey
            #with cProfile.Profile() as pr:
            #    balance(args, state)
            #    pr.print_stats(SortKey.CUMULATIVE)

        elif args.mode == 'show':
            show(args, state)

        elif args.mode == 'showremapped':
            showremapped(args, state)

        elif args.mode == 'poolosddiff':
            poolosddiff(args, state)

        elif args.mode == 'repairstats':
            repairstats(args, state)

        else:
            raise Exception(f"unknown mode: {args.mode}")

if __name__ == "__main__":
    main()
