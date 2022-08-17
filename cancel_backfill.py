#!/usr/bin/env python3

from subprocess import DEVNULL, check_output
from json import loads

from setuptools import Command

def jsoncall(cmd, swallow_stderr=False):
    if not isinstance(cmd, list):
        raise ValueError("need cmd as list")
    stderrval = DEVNULL if swallow_stderr else None
    rawdata = check_output(cmd, stderr=stderrval)
    return loads(rawdata.decode())

def get_osd_dump():
    command = "ceph osd dump -f json".split()
    return jsoncall(command)

def get_pg_query(pgid):
    command = f"ceph pg {pgid} query -f json".split()
    return jsoncall(command)

def find_remapped_list(osd_dump):
    pg_upmap_items = {}
    pg_with_upmap = [x['pgid'] for x in osd_dump["pg_temp"]]
    for item in osd_dump["pg_upmap_items"]:
        if item['pgid'] in pg_with_upmap:
            osds = item["mappings"]
            remapped_list_for_osd = {"map" : [], "stats" : ""}
            for osd in osds:
                remapped_list_for_osd["map"].append(
                    (osd['from'], osd['to'])
                )
            pg_upmap_items[item['pgid']] = remapped_list_for_osd
    return pg_upmap_items

def find_current_remapped_pg(pg_upmap_items):
    for pg in pg_upmap_items:
        pg_detail = get_pg_query(pg)
        for up, act in zip(pg_detail["up"], pg_detail["acting"]):
            if up != act:
                pg_upmap_items[pg]["map"].append((up,act))
        pg_upmap_items[pg]["stats"] = pg_detail["state"]

def main():
    osd_dump = get_osd_dump()
    pg_upmap_items = find_remapped_list(osd_dump)
    find_current_remapped_pg(pg_upmap_items)
    
    active_backfill = []
    waiting_backfill= []

    for item in pg_upmap_items:
        if pg_upmap_items[item]['stats'] == "active+remapped+backfilling":
            active_backfill.append(
                "ceph osd pg-upmap-items %s %s"
                %(
                    item,
                    " ".join(f"{x} {y}" for x,y in pg_upmap_items[item]["map"]),
                )
            )
        elif pg_upmap_items[item]['stats'] == "active+remapped+backfill_wait":
            waiting_backfill.append(
                "ceph osd pg-upmap-items %s %s"
                %(
                    item,
                    " ".join(f"{x} {y}" for x,y in pg_upmap_items[item]["map"]),
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


if __name__ == "__main__":
    main()
