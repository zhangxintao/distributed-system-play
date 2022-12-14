package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Leader    int = 1
	Follower      = 2
	Candidate     = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	clientCh  chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeatTime time.Time
	role              int
	termVoteCount     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.role == Leader

	log.Printf("%v - getstate:%v, %v \n", rf.me, term, isleader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	log.Printf("%v - info: persist values, rf.currentTerm:%+v, rf.votedFor:%+v, rf.logs:%+v", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("%v - error: missing values, currentTerm:%+v,voteFor:%+v, logs:%+v", rf.me, currentTerm, votedFor, logs)
	} else {
		log.Printf("%v - info: read values, currentTerm:%+v,voteFor:%+v, logs:%+v", rf.me, currentTerm, votedFor, logs)
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingTerm  int
	ConflictingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v - processing append entries request:%+v \n", rf.me, args)
	log.Printf("%v - states before append entries:%+v \n", rf.me, rf)

	// check request from legal leader
	if rf.currentTerm > args.Term {
		log.Printf("%v - higher term in current \n", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		log.Printf("%v - higher term in appendentries \n", rf.me)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	if rf.role == Leader || rf.role == Candidate {
		log.Printf("%v - duplicate leader detected for same term:%v \n", rf.me, rf.currentTerm)
		return
	}

	log.Printf("%v - legal appendentries from leader, update last heartbeat and role\n", rf.me)
	rf.lastHeartbeatTime = time.Now()
	rf.role = Follower

	if args.PrevLogIndex > len(rf.logs)-1 {
		log.Printf("%v - request PrevLogIndex is after current log entries \n", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		conflictingTerm := rf.logs[len(rf.logs)-1].Term
		firstIndexOfConflict := len(rf.logs) - 1
		for rf.logs[firstIndexOfConflict].Term == conflictingTerm && firstIndexOfConflict > 0 {
			firstIndexOfConflict--
		}
		reply.ConflictingIndex = firstIndexOfConflict + 1
		reply.ConflictingTerm = conflictingTerm
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Printf("%v - term does not match in PrevLogIndex:%v \n", rf.me, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		conflictingTerm := rf.logs[args.PrevLogIndex].Term
		firstIndexOfConflict := args.PrevLogIndex
		for rf.logs[firstIndexOfConflict].Term == conflictingTerm && firstIndexOfConflict > 1 {
			firstIndexOfConflict--
		}
		reply.ConflictingIndex = firstIndexOfConflict + 1
		reply.ConflictingTerm = conflictingTerm
		return
	}

	currentLogIndex := args.PrevLogIndex + 1
	appendEntryIndex := 0
	conflictIndex := -1
	for currentLogIndex < len(rf.logs) && appendEntryIndex < len(args.Entries) {
		if rf.logs[currentLogIndex] != args.Entries[appendEntryIndex] {
			conflictIndex = currentLogIndex
			break
		}
		currentLogIndex++
		appendEntryIndex++
	}
	if conflictIndex != -1 {
		// has conflict
		rf.logs = rf.logs[0:conflictIndex]
	}
	rf.logs = append(rf.logs, args.Entries[appendEntryIndex:len(args.Entries)]...)
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if oldCommitIndex != rf.commitIndex {
			for oldCommitIndex < rf.commitIndex {
				commitMsg := ApplyMsg{CommandValid: true, CommandIndex: oldCommitIndex + 1, Command: rf.logs[oldCommitIndex+1].Command}
				rf.clientCh <- commitMsg
				log.Printf("%v - info: applied:%+v to ch \n", rf.me, commitMsg)
				rf.lastApplied = rf.commitIndex
				oldCommitIndex++
			}
		}
	}

	reply.Success = true
	log.Printf("%v - states after append entries:%+v \n", rf.me, rf)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v - processing request vote, args:%+v \n", rf.me, args)

	if rf.currentTerm > args.Term {
		log.Printf("%v - higher term in current, payload:%+v \n", rf.me, args)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		log.Printf("%v - has voted for server:%v in term:%v, payload:%+v\n", rf.me, rf.votedFor, rf.currentTerm, args)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		log.Printf("%v - higher term in request:%v \n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	reply.VoteGranted = rf.checkLogLeastUpToDate(args)
	if reply.VoteGranted {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.role = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
	}
}

func (rf *Raft) checkLogLeastUpToDate(args *RequestVoteArgs) bool {
	lastTerm := rf.logs[len(rf.logs)-1].Term
	if lastTerm < args.LastLogTerm {
		return true
	} else if lastTerm > args.LastLogTerm {
		return false
	}

	if len(rf.logs)-1 > args.LastLogIndex {
		return false
	}

	return true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	log.Printf("%v -info: receive command in Start:%+v\n", rf.me, command)
	// Your code here (2B).
	rf.mu.Lock()
	rf.mu.Unlock()

	if rf.role != Leader {
		isLeader = false
	} else {
		log.Printf("%v -info: start appending command to leader:%v, state before:%+v \n", rf.me, command, rf)
		index = len(rf.logs)
		term = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
		rf.matchIndex[rf.me] = index
		log.Printf("%v -info: after appending command to leader:%v, state after:%+v \n", rf.me, command, rf)
		rf.persist()
	}

	log.Printf("%v -info: response to client:%v, %v, %v\n", rf.me, index, term, isLeader)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		sleepTime := rand.Intn(400-200) + 200
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		//log.Printf("%v -info: current state:%+v \n", rf.me, rf)
		if rf.isTimeout(int64(sleepTime)) {
			log.Printf("%v -info: timeout detected \n", rf.me)
			// start election
			rf.startElection()
		}
	}
}

func (rf *Raft) isTimeout(sleepTime int64) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().Sub(rf.lastHeartbeatTime).Milliseconds() >= sleepTime
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		log.Printf("%v -info: Leader doesn't support election \n", rf.me)
		return
	}

	if rf.role == Follower {
		log.Printf("%v -info: transit to candidate and increase term for election \n", rf.me)
		rf.role = Candidate
	}

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.termVoteCount = 1
	rf.persist()

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.logs) - 1, LastLogTerm: rf.logs[len(rf.logs)-1].Term}
	reply := RequestVoteReply{}
	go func(args RequestVoteArgs, reply RequestVoteReply) {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(targetServer int, args RequestVoteArgs, reply RequestVoteReply) {
					log.Printf("%v -info: request vote from: %v \n", rf.me, targetServer)
					result := rf.sendRequestVote(targetServer, &args, &reply)
					if result {
						log.Printf("%v -info: request vote from: %v call succeed with reply:%+v \n", rf.me, targetServer, reply)
						if reply.VoteGranted {
							log.Printf("%v -info: request vote from: %v granted, payload:%+v \n", rf.me, targetServer, reply)
							rf.tryBeLeader(args.Term)
						} else {
							rf.handleTermConflict(reply.Term)
						}
					} else {
						log.Printf("%v -error: request vote from: %v call failed \n", rf.me, targetServer)
					}
				}(i, args, reply)
			}
		}
	}(args, reply)
}

func (rf *Raft) tryBeLeader(tryTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		log.Printf("%v -info: %v is not supported to be leader \n", rf.me, rf.role)
		return
	}

	if rf.currentTerm != tryTerm {
		log.Printf("%v -info: detected out-of-date try be leader request for term:%v, currentterm:%v \n", rf.me, tryTerm, rf.currentTerm)
	} else {
		log.Printf("%v -info: get vote for term:%v \n", rf.me, tryTerm)
		rf.termVoteCount++
		if rf.termVoteCount > len(rf.peers)/2 {
			log.Printf("%v -info: become leader \n", rf.me)
			rf.role = Leader
			go rf.appendEntries()
		}
	}
}

func (rf *Raft) appendEntries() {
	for rf.killed() == false && rf.role == Leader {
		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(targetServer int) {
						//rf.mu.Lock()
						//defer rf.mu.Unlock()
						/*
							if rf.nextIndex[targetServer] < rf.matchIndex[targetServer] {
								rf.nextIndex[targetServer] = rf.matchIndex[targetServer] + 1
							}
						*/

						rf.mu.Lock()
						if rf.nextIndex[targetServer]-1 >= len(rf.logs) {
							log.Printf("%v -prevLogIndex out of range, give up the execution for s: %v\n", rf.me, targetServer)
							rf.mu.Unlock()
							return
						}
						nextIndex := rf.nextIndex[targetServer]
						prevLogIndex := nextIndex - 1
						prevLogTerm := -1
						entries := []LogEntry{}
						prevLogTerm = rf.logs[prevLogIndex].Term
						entries = rf.logs[nextIndex:len(rf.logs)]
						if rf.nextIndex[targetServer] == nextIndex {
							rf.nextIndex[targetServer] += len(entries)
						}

						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: rf.commitIndex}
						reply := AppendEntriesReply{}
						rf.mu.Unlock()

						ok := rf.sendAppendEntries(targetServer, &args, &reply)
						if ok {
							//process result
							log.Printf("%v -info: appendEntries to: %v succeed, reply:%+v \n", rf.me, targetServer, reply)
							rf.handleAppendEntriesReply(args, reply, targetServer)
						} else {
							log.Printf("%v -error: appendEntries to: %v failed \n", rf.me, targetServer)
							rf.handleAppendEntriesRPCFailure(prevLogIndex, prevLogTerm, targetServer)
						}
					}(i)
				}
			}
		}()

		sleepTime := 150
		log.Printf("%v - info: next heartbeat to sleep: %v millisecond \n", rf.me, sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}
func (rf *Raft) handleAppendEntriesRPCFailure(prevLogIndex int, prevLogTerm int, targetServer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v - info: handling rpc failure in append entry, prevLogIndex:%+v, prevLogTerm:%+v\n", rf.me, prevLogIndex, prevLogTerm)
	if len(rf.logs) > prevLogIndex && rf.logs[prevLogIndex].Term == prevLogTerm && rf.matchIndex[targetServer] < rf.nextIndex[targetServer] && rf.nextIndex[targetServer] > prevLogIndex {
		rf.nextIndex[targetServer] = prevLogIndex + 1
	}
}
func (rf *Raft) handleAppendEntriesReply(args AppendEntriesArgs, reply AppendEntriesReply, targetServer int) {
	log.Printf("%v -info: processing reply:%+v, from:%v \n", rf.me, reply, targetServer)
	if reply.Success {
		rf.handleSuccessAppend(args, reply, targetServer)
		return
	}

	rf.handleTermConflict(reply.Term)
	rf.handlePrevIndexConflict(reply, args, targetServer)
}
func (rf *Raft) handleSuccessAppend(args AppendEntriesArgs, reply AppendEntriesReply, targetServer int) {
	if reply.Success == false {
		return
	}

	if len(args.Entries) == 0 {
		log.Printf("%v -info: skip append handling for heartbeat \n", rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		log.Printf("%v -info: only leader is eligible for handling succeed append entries \n", rf.me)
		return
	}

	log.Printf("%v -info: processing succeed reply:%+v, from:%v \n", rf.me, reply, targetServer)
	rf.matchIndex[targetServer] = args.PrevLogIndex + len(args.Entries)

	// try to set commitIndex of leader
	count := 0
	N := rf.matchIndex[targetServer]
	log.Printf("%v -info: trying to update matchindex to %v \n", rf.me, N)
	if N <= rf.commitIndex {
		log.Printf("%v -info: current commitIndex:%v greater than match index, skip the update \n", rf.me, rf.commitIndex)
		return
	}

	if rf.logs[N].Term != rf.currentTerm {
		log.Printf("%v -info: cannot update commitIndex when term conflict, n-term:%v, current:%v \n", rf.me, rf.logs[N].Term, rf.currentTerm)
		return
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		if N <= rf.matchIndex[i] {
			count++
		}
	}
	if count > len(rf.matchIndex)/2 {
		log.Printf("%v -info: %v match criteria \n", rf.me, N)
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = N
		for oldCommitIndex < rf.commitIndex {
			commitMsg := ApplyMsg{CommandValid: true, CommandIndex: oldCommitIndex + 1, Command: rf.logs[oldCommitIndex+1].Command}
			rf.clientCh <- commitMsg
			log.Printf("%v -info: applied:%+v to ch \n", rf.me, commitMsg)
			rf.lastApplied = oldCommitIndex
			oldCommitIndex++
		}
	}
}
func (rf *Raft) handleTermConflict(replyTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v -info: processing potential term conflict, reply:%v, current:%v \n", rf.me, replyTerm, rf.currentTerm)
	if rf.currentTerm < replyTerm {
		log.Printf("%v -info: detected higher term:%v \n", rf.me, replyTerm)
		rf.currentTerm = replyTerm
		rf.votedFor = -1
		rf.termVoteCount = 0
		rf.role = Follower
		rf.persist()
	}
}

func (rf *Raft) handlePrevIndexConflict(reply AppendEntriesReply, args AppendEntriesArgs, targetServer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		log.Printf("%v -info: only leader is eligible for processing previndex conflict, current state:%v, targetserver:%v \n", rf.me, rf, targetServer)
		return
	}

	if rf.nextIndex[targetServer] > reply.ConflictingIndex && reply.ConflictingIndex > 0 {
		log.Printf("%v -info: processing previndex conflict, reply:%v, targetserver:%v \n", rf.me, reply, targetServer)
		rf.nextIndex[targetServer] = reply.ConflictingIndex
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.clientCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{
		Command: nil,
		Term:    0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.lastHeartbeatTime = time.Now()
	rf.role = Follower
	rf.termVoteCount = 0

	log.Printf("%v - info: init done, current state:%+v \n", rf.me, rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
