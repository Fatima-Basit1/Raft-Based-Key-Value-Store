package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

//-----------------------
// Raft types and constants
//-----------------------

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int     `json:"term"`
	Command Command `json:"command"`
}

type Command struct {
	Operation string `json:"operation"` // "put" or "append"
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// RPC structures
type RequestVoteArgs struct {
	Term         int `json:"term"`
	CandidateId  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderId     int        `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

//-----------------------
// Raft Node
//-----------------------

type RaftNode struct {
	mu                 sync.Mutex
	currentTerm        int
	votedFor           int
	log                []LogEntry
	commitIndex        int
	lastApplied        int
	nextIndex          map[int]int
	matchIndex         map[int]int
	state              RaftState
	id                 int
	peers              []string // list of peer addresses ("host:port")
	port               string
	electionResetEvent time.Time
	store              map[string]string // key-value store
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//-----------------------
// Main: Start HTTP server, election timer, and CLI
//-----------------------

func main() {
	// Parse command-line arguments
	id := flag.Int("id", 0, "node id (integer starting at 0)")
	port := flag.String("port", "8000", "port to listen on")
	peersStr := flag.String("peers", "", "comma-separated list of peer addresses (host:port)")
	flag.Parse()
	peers := strings.Split(*peersStr, ",")

	node := &RaftNode{
		currentTerm:        0,
		votedFor:           -1,
		log:                make([]LogEntry, 0),
		commitIndex:        -1,
		lastApplied:        -1,
		state:              Follower,
		id:                 *id,
		peers:              peers,
		port:               *port,
		electionResetEvent: time.Now(),
		store:              make(map[string]string),
	}

	// HTTP endpoints for Raft RPCs and client commands (if needed)
	http.HandleFunc("/requestVote", node.handleRequestVote)
	http.HandleFunc("/appendEntries", node.handleAppendEntries)
	http.HandleFunc("/put", node.handlePut)
	http.HandleFunc("/append", node.handleAppend)
	http.HandleFunc("/get", node.handleGet)

	// Start the election timer in a separate goroutine
	go node.runElectionTimer()

	// Start the HTTP server in a goroutine
	go func() {
		log.Printf("Starting node %d on port %s\n", node.id, node.port)
		log.Fatal(http.ListenAndServe(":"+node.port, nil))
	}()

	// Start interactive CLI for client commands
	node.runCLI()
}

//-----------------------
// Election timer and election logic
//-----------------------

// runElectionTimer now uses a fresh timeout and then checks the current state
func (rn *RaftNode) runElectionTimer() {
	for {
		// Set a random timeout between 300 and 500 milliseconds
		timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
		time.Sleep(timeout)

		rn.mu.Lock()
		// If not leader and enough time has elapsed since the last election reset, start election
		if rn.state != Leader && time.Since(rn.electionResetEvent) >= timeout {
			rn.startElection()
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) startElection() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	// Reset election timer
	rn.electionResetEvent = time.Now()
	votesReceived := 1
	log.Printf("[Node %d] Starting election for term %d\n", rn.id, rn.currentTerm)

	for i, peer := range rn.peers {
		// Skip self; assumes peers list order matches node IDs.
		if i == rn.id {
			continue
		}
		go func(peer string) {
			rn.mu.Lock()
			lastLogIndex := len(rn.log) - 1
			lastLogTerm := 0
			if lastLogIndex >= 0 {
				lastLogTerm = rn.log[lastLogIndex].Term
			}
			args := RequestVoteArgs{
				Term:         rn.currentTerm,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			rn.mu.Unlock()

			data, _ := json.Marshal(args)
			resp, err := http.Post("http://"+peer+"/requestVote", "application/json", bytes.NewBuffer(data))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var reply RequestVoteReply
			json.NewDecoder(resp.Body).Decode(&reply)

			rn.mu.Lock()
			defer rn.mu.Unlock()
			if reply.VoteGranted {
				votesReceived++
				// If a majority is achieved, become leader.
				if rn.state == Candidate && votesReceived > len(rn.peers)/2 {
					log.Printf("[Node %d] Became leader for term %d\n", rn.id, rn.currentTerm)
					rn.state = Leader
					// Initialize nextIndex and matchIndex for each follower.
					rn.nextIndex = make(map[int]int)
					rn.matchIndex = make(map[int]int)
					for idx := range rn.peers {
						rn.nextIndex[idx] = len(rn.log)
						rn.matchIndex[idx] = 0
					}
					go rn.runHeartbeatTimer()
				}
			} else if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.state = Follower
				rn.votedFor = -1
			}
		}(peer)
	}
}

//-----------------------
// Heartbeat and log replication
//-----------------------

func (rn *RaftNode) runHeartbeatTimer() {
	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()
		rn.broadcastHeartbeat()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rn *RaftNode) broadcastHeartbeat() {
	for i, peer := range rn.peers {
		if i == rn.id {
			continue
		}
		go func(peer string, followerId int) {
			rn.mu.Lock()
			prevLogIndex := rn.nextIndex[followerId] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}
			args := AppendEntriesArgs{
				Term:         rn.currentTerm,
				LeaderId:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{}, // heartbeat (no entries)
				LeaderCommit: rn.commitIndex,
			}
			rn.mu.Unlock()

			data, _ := json.Marshal(args)
			resp, err := http.Post("http://"+peer+"/appendEntries", "application/json", bytes.NewBuffer(data))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var reply AppendEntriesReply
			json.NewDecoder(resp.Body).Decode(&reply)

			rn.mu.Lock()
			defer rn.mu.Unlock()
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.state = Follower
				rn.votedFor = -1
			}
			if reply.Success {
				// Update follower indices on success.
				rn.nextIndex[followerId] = len(rn.log)
				rn.matchIndex[followerId] = rn.nextIndex[followerId] - 1
			} else {
				// If log inconsistency, decrement nextIndex and retry later.
				rn.nextIndex[followerId]--
			}
		}(peer, i)
	}
}

func (rn *RaftNode) replicateLogEntry(index int) {
	votes := 1 // leader already has the entry
	var wg sync.WaitGroup

	for i, peer := range rn.peers {
		if i == rn.id {
			continue
		}
		wg.Add(1)
		go func(peer string, followerId int) {
			defer wg.Done()
			rn.mu.Lock()
			prevLogIndex := rn.nextIndex[followerId] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}
			// Send all entries from follower’s nextIndex onward.
			entries := rn.log[rn.nextIndex[followerId]:]
			args := AppendEntriesArgs{
				Term:         rn.currentTerm,
				LeaderId:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rn.commitIndex,
			}
			rn.mu.Unlock()

			data, _ := json.Marshal(args)
			resp, err := http.Post("http://"+peer+"/appendEntries", "application/json", bytes.NewBuffer(data))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var reply AppendEntriesReply
			json.NewDecoder(resp.Body).Decode(&reply)

			rn.mu.Lock()
			defer rn.mu.Unlock()
			if reply.Success {
				rn.nextIndex[followerId] = len(rn.log)
				rn.matchIndex[followerId] = rn.nextIndex[followerId] - 1
				votes++
			} else {
				rn.nextIndex[followerId]--
			}
		}(peer, i)
	}
	wg.Wait()

	// If a majority have replicated the entry, commit it.
	if votes > len(rn.peers)/2 {
		rn.commitIndex = index
		// Apply committed entries to the state machine (key-value store).
		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			command := rn.log[rn.lastApplied].Command
			if command.Operation == "put" {
				rn.store[command.Key] = command.Value
			} else if command.Operation == "append" {
				rn.store[command.Key] += command.Value
			}
		}
	}
}

//-----------------------
// RPC Handlers
//-----------------------

func (rn *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := RequestVoteReply{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}
	if args.Term < rn.currentTerm {
		json.NewEncoder(w).Encode(reply)
		return
	}
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = -1
		rn.state = Follower
	}
	// Check if candidate’s log is at least as up-to-date as our log.
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}
	upToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && upToDate {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
		// Reset election timer on granting vote.
		rn.electionResetEvent = time.Now()
	}
	json.NewEncoder(w).Encode(reply)
}

func (rn *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    rn.currentTerm,
		Success: false,
	}
	if args.Term < rn.currentTerm {
		json.NewEncoder(w).Encode(reply)
		return
	}
	// Reset election timer on valid AppendEntries RPC.
	rn.electionResetEvent = time.Now()
	rn.state = Follower
	rn.currentTerm = args.Term

	// Check log consistency.
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rn.log) || rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			json.NewEncoder(w).Encode(reply)
			return
		}
	}
	// Append any new entries not already in the log.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rn.log) {
			rn.log[index] = entry
		} else {
			rn.log = append(rn.log, entry)
		}
	}
	// Update commitIndex if needed.
	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
		// Apply newly committed entries.
		for rn.lastApplied < rn.commitIndex {
			rn.lastApplied++
			command := rn.log[rn.lastApplied].Command
			if command.Operation == "put" {
				rn.store[command.Key] = command.Value
			} else if command.Operation == "append" {
				rn.store[command.Key] += command.Value
			}
		}
	}
	reply.Success = true
	json.NewEncoder(w).Encode(reply)
}

//-----------------------
// HTTP Client Command Handlers (for curl, if desired)
//-----------------------

func (rn *RaftNode) handlePut(w http.ResponseWriter, r *http.Request) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		http.Error(w, "Not leader", http.StatusTemporaryRedirect)
		return
	}
	rn.mu.Unlock()

	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	cmd.Operation = "put"
	// Append command to leader’s log.
	rn.mu.Lock()
	entry := LogEntry{Term: rn.currentTerm, Command: cmd}
	rn.log = append(rn.log, entry)
	index := len(rn.log) - 1
	rn.mu.Unlock()

	// Replicate the log entry to followers.
	rn.replicateLogEntry(index)

	// Apply the command to the state machine.
	rn.mu.Lock()
	rn.store[cmd.Key] = cmd.Value
	rn.mu.Unlock()

	w.Write([]byte("OK"))
}

func (rn *RaftNode) handleAppend(w http.ResponseWriter, r *http.Request) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		http.Error(w, "Not leader", http.StatusTemporaryRedirect)
		return
	}
	rn.mu.Unlock()

	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	cmd.Operation = "append"
	// Append command to leader’s log.
	rn.mu.Lock()
	entry := LogEntry{Term: rn.currentTerm, Command: cmd}
	rn.log = append(rn.log, entry)
	index := len(rn.log) - 1
	rn.mu.Unlock()

	// Replicate the log entry.
	rn.replicateLogEntry(index)

	// Apply the command to the state machine.
	rn.mu.Lock()
	rn.store[cmd.Key] += cmd.Value
	rn.mu.Unlock()

	w.Write([]byte("OK"))
}

func (rn *RaftNode) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	rn.mu.Lock()
	value, ok := rn.store[key]
	rn.mu.Unlock()
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(value))
}

//-----------------------
// Interactive CLI for Terminal Input
//-----------------------

func (rn *RaftNode) runCLI() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Interactive CLI started. Commands:")
	fmt.Println("  put <key> <value>")
	fmt.Println("  append <key> <value>")
	fmt.Println("  get <key>")
	fmt.Println("  exit")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		tokens := strings.Fields(line)
		if len(tokens) == 0 {
			continue
		}
		cmdType := strings.ToLower(tokens[0])
		switch cmdType {
		case "exit":
			fmt.Println("Exiting CLI.")
			os.Exit(0)
		case "put":
			if len(tokens) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key := tokens[1]
			value := strings.Join(tokens[2:], " ")
			rn.processPut(key, value)
		case "append":
			if len(tokens) < 3 {
				fmt.Println("Usage: append <key> <value>")
				continue
			}
			key := tokens[1]
			value := strings.Join(tokens[2:], " ")
			rn.processAppend(key, value)
		case "get":
			if len(tokens) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := tokens[1]
			rn.processGet(key)
		default:
			fmt.Println("Unknown command. Valid commands: put, append, get, exit")
		}
	}
}

func (rn *RaftNode) processPut(key, value string) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		fmt.Println("Not leader. Please try again on the leader node.")
		return
	}
	cmd := Command{Operation: "put", Key: key, Value: value}
	entry := LogEntry{Term: rn.currentTerm, Command: cmd}
	rn.log = append(rn.log, entry)
	index := len(rn.log) - 1
	rn.mu.Unlock()

	rn.replicateLogEntry(index)

	rn.mu.Lock()
	rn.store[key] = value
	rn.mu.Unlock()
	fmt.Println("Put operation successful.")
}

func (rn *RaftNode) processAppend(key, value string) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		fmt.Println("Not leader. Please try again on the leader node.")
		return
	}
	cmd := Command{Operation: "append", Key: key, Value: value}
	entry := LogEntry{Term: rn.currentTerm, Command: cmd}
	rn.log = append(rn.log, entry)
	index := len(rn.log) - 1
	rn.mu.Unlock()

	rn.replicateLogEntry(index)

	rn.mu.Lock()
	rn.store[key] += value
	rn.mu.Unlock()
	fmt.Println("Append operation successful.")
}

func (rn *RaftNode) processGet(key string) {
	rn.mu.Lock()
	value, ok := rn.store[key]
	rn.mu.Unlock()
	if !ok {
		fmt.Println("Key not found.")
	} else {
		fmt.Printf("Value for key '%s': %s\n", key, value)
	}
}
