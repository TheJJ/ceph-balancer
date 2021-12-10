JJ's Ceph Balancer
==================

[Ceph](https://ceph.io)'s "main" design issue is equal data placement.
One mitigation is the [mgr balancer](https://docs.ceph.com/en/latest/rados/operations/balancer/).

This is an alternative Ceph balancer implementation.
* The upstream balancer optimizes for (weighted) equal number of PGs on each OSD for each pool.
* This balancer optimizes for equal OSD storage utilization and PG placement accross all pools.

For most clusters, the `mgr balancer` works well.
For heterogeneous clusters with a lot of device and server capacity variance _and_ many pools, placement may be very bad - the reason this balancer was created.


## How?

* [Ceph Overview](https://docs.ceph.com/en/latest/start/intro/)
* [Ceph Cheatsheet](https://github.com/TheJJ/ceph-cheatsheet)


### Why balancing?

A [data pool](https://docs.ceph.com/en/latest/rados/operations/pools/) is split into placement groups (PGs).
The number of PGs is configured via the `pg_num` pool attribute.
Hence, one PG roughly has size `pg_size = pool_size/pg_num`.

PGs are placed on OSDs (a disk), using constraints defined via [CRUSH](https://docs.ceph.com/en/latest/rados/operations/crush-map/).
Usually PGs are spread accross servers, so that when one server goes down, enough disks are in other servers so the data remains available.
The number of OSDs for one PG is configured via the pools `size` property.

There's two pool kinds: replica and [erasure coded](https://en.wikipedia.org/wiki/Erasure_code).

The utilized space one one OSD is the sum of all PG shards that are stored on it.
A PG shard is the data of one of the participarting OSDs in one PG.
For replica-pools, the shard size equals `pg_size / pool_size` - one full copy.
For ec-pools, the shard size equals `pg_size / (pool_size * k)`, k is the number of data chunks in the ec profile.

CRUSH organizes all the datacenters, racks, servers, OSDs in a tree structure.
Each subtree usually has the weight of all the OSDs below it, and PGs are now distributed evenly, weighted by the sub-tree size at each tree level.
That way big servers/racks/datacenters/... get more data than small ones, but the relative amount is the same.

In theory, each OSD should thus be filled exactly the same relative amount, all are e.g. 30% full.
In practice, not so much:

The cluster, which was the motivation to create this balancer, had devices (same device class, weighted at 1.0) ranging from 55% to 80% size utilization.

The reason is this: The cluster has many pools of different sizes (at time of writing 46), OSD sizes vary from 1T to 14T, 4 to 40 OSDs per server.
And the `mgr balancer` can't handle this.


### `mgr balancer`

Ceph's included balancer optimizes by PG count on devices.
It does so by analyzing each pool indepently, and then tries to move each pool's PGs so that each participating device has equal normalized PG counts.
Normalized means placing double the PGs on a double-sized OSD.

Example: Placing 600 PGs on a 2T and 4T OSD means each 1T gets `600PGs/(4T+2T) = 100PGs/T`, thus the 2T OSD gets 200PGs, the 4T OSD 600.

PG counts are powers of two, so distributing them really equally will almost never work.

Because of this, the best possible solution is some OSDs having an offset of 1 PG to the ideal count.
As a PG-distribution-optimization is done per pool, without checking other pool's distribution at all, some devices will be the `+1` more often than others.
At worst one OSD is the `+1` **for each** pool in the cluster.

This OSD can then end up, for example, being 80% full, since it is a `+1` for 20 times.
The one that never is the `+1` is 50% full.
That's bad and not balanced at all.

Additionally, the shard sizes of PGs are not equal - they shrink and grow with pool usage, whereas the PG count will remain exactly the same, which the mgr-balancer uses, so it does nothing.

To make things worse, if there's a huge server in the cluster which is so big, CRUSH can't place data often enough on it to fill it to the same level as any other server, the balancer will fail moving PGs across servers that actually would have space.
This happens since it sees only this server's OSDs as "underfull", but each PG has one shard on that server already, so no data can be moved on it. Even though there are likely other OSDs in the cluster (which may have variations from 90% to 50% full, and determine the available space because one of them is the fullest overall), these other OSDs are not balanced - the only considered balancing target is the big empty server.

So we have two main issues:
* If you have multiple bigger pools, the `+1`-PG placement does not consider the global view (i.e. where other pools place the +1 PGs)
* If there's too-empty buckets (which can't be filled more because of crush constraints), other buckets are no longer balanced


### jj-balancer

To solve this, the main optimization goal is equal OSD utilization:

Generate candidate PG movements and validate them against the crush constraints, PG counts, utilization estimations, ...
To get PG candiates, order all OSDs by utilization (optionally only for one crush root).
Utilization is estimated from all PGs where the OSD is in the `up` set (due to ongoing partial PG transfers).
From the fullest OSD, try to move a "suitable" PG shard on it to the least-utilized OSD.
If this violates constraints, try the next least-utilized OSD, and so on, or try a different PG.

Once a suitable OSD is found, check if the new placment decreases the cluster utilization variance.
If this is the case, record the PG movement and try to move another PG with the same approach.

That way the balancer generates new "upmap items", i.e. movement instructions for a PG from some OSDs to better ones, which you can apply if you're satisfied with the results.

If this is done forever, all OSDs will have very little utilization variance, or CRUSH constraints prevent us from doing more PG movements.

Simplified pseudocode:

```python
# given an osd we want to empty, which pg do we select?
def get_pg_move_candidates(sourceosd):
    def compare(pg, otherpg):
        return (pg.remapped > otherpg.remapped or
                pg.upmap_item_count < otherpg.upmap_item_count or
                pg.size > otherpg.size)
    return sort(pgs_on_osd(sourceosd), compare)

try_limit = 1
max_movements = 10
while True:
    next_move:
    if len(movements) >= max_movements:
        stop()

    # try to empty the fullest osds
    for i, from_osd in enumerate(osds_by_utilization_asc(crushroot)):
        if i > try_limit:
            finish('could not empty {try_limit} fullest devices, stopping')
            stop()

        # try a suitable PG to move it away from a full osd
        for pg in get_pg_move_candidates(from_osd):
            # move it to an empty osd
            for to_osd in osds_by_utilization_desc(candidate_osds_for(pg)):

                # only move if constraints allow it
                if (is_crush_move_valid(pg, from_osd, to_osd) and
                    respects_pool_balance(pg, from_osd, to_osd) and
                    cluster_utilization_variance_is_better(pg, from_osd, to_osd)):

                    movements.append((pg, from_osd, to_osd))
                    goto next_move

# resulting movements
for movement in movements:
    print(f"ceph osd pg-upmap-items {generate_upmap(movement)}")
```

Runtime:
* Worst-case (the fullest OSD can't be emptied more): `O(OSDs * PGs)`
* If, after that, we tried the second-to fullest, third, and all others, it would be: `O(OSDs²*PGs)`

Likely this can be optimized further.


## Usage

```
# to generate max 10 pg movements:
./placementoptimizer.py -v balance --max-pg-moves 10 | tee /tmp/balance-upmaps

# -> if you're satisfied, run bash /tmp/balance-upmaps

# but it can do more than balance!
./placementoptimizer.py --help
```

## Contributions

The script is not the prettiest (yet), but produces balancing-improvement movements.

Ideally, with some further improvements and tuning, it could be integrated in upstream-Ceph as an alternative balancer implementation.

So if you have any idea and suggestion how to improve things, please submit issues and [pull requests](https://github.com/TheJJ/ceph-balancer/pulls).


## Contact

If you have questions, suggestions, encounter any problem,
please join our Matrix or IRC channel and ask!

* Matrix Chat: [`#sfttech:matrix.org`](https://matrix.to/#/#sfttech:matrix.org)
* IRC Chat: [`libera.chat #sfttech`](https://web.libera.chat/#sfttech)

Of course, create [issues](https://github.com/TheJJ/ceph-balancer/issues)
and [pull requests](https://github.com/TheJJ/ceph-balancer/pulls).


### License

Released under the **GNU General Public License** version 3 or later,
see [COPYING](COPYING) and [LICENSE](LICENSE) for details.
