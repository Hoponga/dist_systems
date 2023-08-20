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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var ELECTION_TIMEOUT_MAX = 200 // in ms

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	myVotes     int // counter for candidates
	votedFor    int
	log         []interface{}

	commitIndex     int
	lastApplied     int
	electionTimeout int64

	totalReceivedVotes int

	nextIndex  []int
	matchIndex []int

	state int // 1 for follower, 2 for candidate, 3 for leader

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == 3
	if isleader {
		//fmt.Printf("%d thinks it's the leader\n", rf.me)
	}
	//fmt.Printf("%d thinks %t isleader", rf.me, isleader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int // 1 if candidate received vote, else 0

}

type AppendEntriesArgs struct {
	Term     int // leader term
	LeaderId int // for server to redirect clients
}

type AppendEntriesReply struct {
	Term    int // follower term
	Success int // for now just always 1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = -1
		reply.Term = rf.currentTerm
	} else if rf.votedFor == -1 || args.Term > rf.currentTerm {
		rf.electionTimeout = 0
		reply.VoteGranted = 1
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm

	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = -1
		reply.Term = rf.currentTerm
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//fmt.Printf("%d handling heartbeat from %d\n", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = -1
	} else {
		reply.Success = 1
		rf.electionTimeout = 0
		if rf.state == 3 {
			fmt.Printf("%d converting to follower...\n", rf.me)
		}
		rf.state = 1
		rf.currentTerm = args.Term

	}
	// if (args.Term > rf.currentTerm && rf.state == 3) || (rf.state != 1 && args.Term >= rf.currentTerm) {

	// 	fmt.Printf("%d converting to follower %d %d \n", rf.me, args.Term, rf.currentTerm)
	// 	reply.Success = 1
	// 	rf.state = 1
	// 	rf.electionTimeout = 0
	// 	reply.Term = rf.currentTerm
	// 	rf.votedFor = -1
	// 	rf.currentTerm = args.Term

	// } else if rf.state == 1 && args.Term >= rf.currentTerm {
	// 	reply.Success = 1
	// 	rf.state = 1
	// 	rf.electionTimeout = 0
	// 	reply.Term = rf.currentTerm
	// 	rf.votedFor = -1
	// 	rf.currentTerm = args.Term
	// } else if args.Term < rf.currentTerm {

	// 	reply.Success = -1
	// 	reply.Term = rf.currentTerm

	// } else {
	// 	fmt.Printf("me is %d, supposed leader is %d, myterm is %d, leader's term is %d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	// }

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		if rf.state != 3 {
			return
		}
		if rf.state == 3 {
			//fmt.Printf("%d is leader\n", rf.me)
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			for i, _ := range rf.peers {

				go func(destination int) {
					if rf.state != 3 {
						return
					}
					if rf.state == 3 && destination != rf.me {
						reply := AppendEntriesReply{}
						//fmt.Printf("%d sending heartbeat to %d\n", rf.me, destination)
						ok := rf.sendAppendEntries(destination, &args, &reply)
						if ok {

						} else {
							//fmt.Printf("%d failed heartbeat to %d\n", rf.me, destination)
						}

					}

				}(i)

			}

		}
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//fmt.Printf("%d is ticking %d\n", rf.me, rf.electionTimeout)

		// Your code here (2A)
		// Check if a leader election should be started.
		fmt.Printf("%d election checking %d %d \n", rf.me, rf.electionTimeout, int64(ELECTION_TIMEOUT_MAX))
		if rf.state != 3 && rf.electionTimeout > int64(ELECTION_TIMEOUT_MAX) {
			rf.electionTimeout = 0
			fmt.Printf("%d Starting election \n", rf.me)
			rf.currentTerm += 1

			rf.state = 2
			rf.myVotes = 1 // myself
			rf.votedFor = rf.me

			neededVotes := int(math.Ceil(float64(len(rf.peers)) / 2.0))
			goodVotes := 1
			totalReceivedVotes := 1
			term := rf.currentTerm

			votechan := make(chan int)

			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				// if our election timeout has ran out,
				if rf.electionTimeout > int64(ELECTION_TIMEOUT_MAX) {
					go rf.ticker()
					return

				}

				go func(destination int) {

					args := RequestVoteArgs{}
					args.CandidateId = rf.me
					args.LastLogIndex = rf.commitIndex
					args.LastLogTerm = rf.lastApplied
					args.Term = rf.currentTerm
					reply := RequestVoteReply{
						Term:        -1,
						VoteGranted: -1,
					}

					ok := rf.sendRequestVote(destination, &args, &reply)

					if ok {

						fmt.Printf("%d Received vote %d at term %d from %d\n", rf.me, reply.VoteGranted, reply.Term, destination)
						votechan <- reply.VoteGranted
						// if reply.VoteGranted != 1 {
						// 	return
						// } else if reply.VoteGranted == 1 {
						// 	rf.mu.Lock()
						// 	defer rf.mu.Unlock()
						// 	rf.myVotes += 1

						// 	// "If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state."
						// 	if reply.Term > rf.currentTerm {
						// 		rf.state = 1
						// 		rf.myVotes = 0

						// 	}

						// 	cond.Broadcast()

						// }

					} else {
						fmt.Printf("rpc to %d went wrong", destination)
						votechan <- reply.VoteGranted

					}

				}(i)

			}
			for {
				result := <-votechan
				totalReceivedVotes++
				if result == 1 {
					goodVotes++

				}
				if goodVotes > len(rf.peers)/2 {

					break
				}
				if totalReceivedVotes >= len(rf.peers) {
					break
				}
				fmt.Printf("%d hi\n", rf.me)
				time.Sleep(10 * time.Millisecond)
			}
			rf.mu.Lock()
			fmt.Printf("%d\n", neededVotes)
			if rf.state != 2 {
				fmt.Printf("%d is no longer a candidate \n", rf.me)
				rf.electionTimeout = 0
				rf.mu.Unlock()

			} else if goodVotes > len(rf.peers)/2 && term == rf.currentTerm {
				rf.state = 3
				rf.electionTimeout = 0
				rf.mu.Unlock()
				fmt.Printf("\n=====%d ELECTED LEADER FOR TERM %d\n", rf.me, rf.currentTerm)
				go rf.heartbeat()

			} else {
				rf.mu.Unlock()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63() % 300)
		rf.electionTimeout += ms
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 1
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
