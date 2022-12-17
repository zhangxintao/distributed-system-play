Key points of handling existing RAFT state in snapshot:
* cannot ahead of commitIndex -> in my case, only proces until commitIndex - 1 for convenience
* commitIndex only move ahead, so Snapshot not need to lock
* Snapshot shouldn't update any RAFT state
* RAFT state should have a calculation based on commitIndex
* commitIndex, nextIndex, and matchIndex will not be impacted by Snapshot
* calculate real log index based on snapshot
* 
* keep the current value of commitIndex, next[] and match[], calculate when needed
    (why? servers are operating individually, however, states used to interact between peers need to be consistent)
* needed -> get log, get log term

When can the snaphsot repalce log?
* the index of snapshot must less than the log length
* the index must less than the commitIndex

Handling "CommitIndex"
* better don't touch it, otherwise, matchIndex[] also need to be manipulated

Handing "nextIndex" and "matchIndex"

Problems/Questions:
* lastApplied only assigned but never used for any purpose
* Snapshot can not use lock pair, why?

TODO:
* add a calculation for commitIndex, next[], match[] based on snapshot index
