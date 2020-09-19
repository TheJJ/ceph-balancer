JJ's Ceph Balancer
==================

[Ceph](https://ceph.io)'s "main" design issue is equal data placement.
One mitigation is the [mgr balancer](https://docs.ceph.com/en/latest/rados/operations/balancer/).

This is an alternative Ceph balancer implementation, with a different strategy than the current (octopus) upstream one: optimizing for equal OSD utilization.

For most clusters, the `mgr balancer` works well.
For heterogeneous clusters with a lot of device and server capacity variance, placement may be very bad - the reason this balancer was created.


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
In practice, not so much.

The cluster that was the motivation to create this has devices (same device class, weighted at 1.0) ranging from 55% to 80%.

The reason is this: The cluster has many pools (at time of writing 37), OSD sizes vary from 1T to 14T, 4 to 40 OSDs per server.
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

This OSD is then then, for example, 80% full.
The one that never is the `+1` is 50% full.

That's bad.

Additionally, the shard sizes of PGs are not equal - they shrink and grow with pool usage, whereas the PG count will remain exactly the same, so the balancer will do nothing.

To make things worse, if there's a huge server in the cluster which is so big, CRUSH can't place data often enough on it to fill it to the same level as any other server, the balancer will fail moving PGs across servers that actually would have space.
This happens since it sees only this server's OSDs as "underfull", but each PG has one shard on that server already, so no data can be moved on it.


### jj-balancer

To solve this, the main optimization goal is equal OSD utilization:

Order all OSDs by utilization (optionally only for one crush root).
From the fullest OSD, try to move the biggest PG shard to the least-utilized OSD.
If this violates constraints, try the next least-utilized OSD, and so on.

If this is done forever, all OSDs will have very little utilization variance.

TODO: explain more :)


## TODOs

Currently, it can only create one PG movement, the goal is of course to create many of them at once.


### Contact

If you have questions, suggestions, encounter any problem,
please join our Matrix or IRC channel and ask!

```
#sfttech:matrix.org
irc.freenode.net #sfttech
```

Of course, create [issues](https://github.com/TheJJ/ceph-balancer/issues)
and [pull requests](https://github.com/TheJJ/ceph-balancer/pulls).


### License

Released under the **GNU General Public License** version 3 or later,
see [COPYING](COPYING) and [LICENSE](LICENSE) for details.
