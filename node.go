package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Role string

const (
	FOLLOWER  Role = "FOLLOWER"
	CANDIDATE Role = "CANDIDATE"
	LEADER    Role = "LEADER"
)

type Node struct {
	mu               sync.Mutex
	id               string
	ballot           int
	votedFor         string
	acceptedBal      int
	acceptedVal      string
	committedVal     string
	role             Role
	leaseExpiry      time.Time
	peers            []string
	resetCh          chan struct{}
	electionAttempts int
}

type NodeList struct {
	mu   sync.RWMutex
	list map[string]*Node
}

func (nl *NodeList) get(id string) *Node {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.list[id]
}

func (nl *NodeList) set(id string, n *Node) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.list[id] = n
}

type PrepareReq struct {
	Ballot   int
	LeaderID string
}

type PrepareResp struct {
	Ballot      int
	AcceptedBal int
	AcceptedVal string
	OK          bool
}

type AcceptReq struct {
	Ballot   int
	LeaderID string
	Value    string
}

type AcceptResp struct {
	Ballot int
	OK     bool
}

type CommitReq struct {
	Ballot   int
	LeaderID string
	Value    string
}

type CommitResp struct {
	Ballot int
	OK     bool
}

type HeartbeatReq struct {
	Ballot   int
	LeaderID string
}

type HeartbeatResp struct {
	Ballot int
	OK     bool
}

type PreVoteReq struct {
	Ballot   int
	LeaderID string
}

type PreVoteResp struct {
	Ballot int
	OK     bool
}

func NewNode(id string, peers []string, nodeList *NodeList) *Node {
	node := &Node{
		id:           id,
		ballot:       0,
		votedFor:     "",
		acceptedBal:  0,
		acceptedVal:  "",
		committedVal: "",
		role:         FOLLOWER,
		leaseExpiry:  time.Time{},
		peers:        peers,
		resetCh:      make(chan struct{}, 1),
	}
	nodeList.set(id, node)
	return node
}

func (n *Node) sendPrepare(nodeList *NodeList) (int, string) {
	n.mu.Lock()
	ballot := n.ballot
	id := n.id
	n.mu.Unlock()

	votes := 1
	type result struct {
		ok          bool
		acceptedBal int
		acceptedVal string
	}
	respCh := make(chan result, len(n.peers))

	for _, peerId := range n.peers {
		targetNode := nodeList.get(peerId)
		go func(node *Node) {
			resp := node.handlePrepare(PrepareReq{
				Ballot:   ballot,
				LeaderID: id,
			})
			respCh <- result{resp.OK, resp.AcceptedBal, resp.AcceptedVal}
		}(targetNode)
	}

	highestAcceptedBal := 0
	recoveredValue := ""

	for i := 0; i < len(n.peers); i++ {
		r := <-respCh
		if r.ok {
			votes++
			if r.acceptedBal > highestAcceptedBal {
				highestAcceptedBal = r.acceptedBal
				recoveredValue = r.acceptedVal
			}
		}
	}

	return votes, recoveredValue
}

func (n *Node) handlePrepare(req PrepareReq) PrepareResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Ballot > n.ballot {
		n.ballot = req.Ballot
		n.role = FOLLOWER
		n.votedFor = req.LeaderID

		n.resetElectionTimer()

		return PrepareResp{
			Ballot:      n.ballot,
			AcceptedBal: n.acceptedBal,
			AcceptedVal: n.acceptedVal,
			OK:          true,
		}
	}
	return PrepareResp{
		Ballot:      n.ballot,
		AcceptedBal: n.acceptedBal,
		AcceptedVal: n.acceptedVal,
		OK:          false,
	}
}

func (n *Node) sendAccept(value string, nodeList *NodeList) bool {
	n.mu.Lock()
	ballot := n.ballot
	id := n.id
	n.mu.Unlock()

	votes := 1
	respCh := make(chan bool, len(n.peers))

	for _, peerId := range n.peers {
		targetNode := nodeList.get(peerId)
		go func(node *Node) {
			resp := node.handleAccept(AcceptReq{
				Ballot:   ballot,
				LeaderID: id,
				Value:    value,
			})
			respCh <- resp.OK
		}(targetNode)
	}

	for i := 0; i < len(n.peers); i++ {
		if <-respCh {
			votes++
		}
	}

	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1
	return votes >= majority
}

func (n *Node) handleAccept(req AcceptReq) AcceptResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Ballot >= n.ballot {
		n.ballot = req.Ballot
		n.acceptedBal = req.Ballot
		n.acceptedVal = req.Value
		return AcceptResp{
			Ballot: n.ballot,
			OK:     true,
		}
	}
	return AcceptResp{
		Ballot: n.ballot,
		OK:     false,
	}
}

func (n *Node) sendCommit(value string, nodeList *NodeList) bool {
	n.mu.Lock()
	ballot := n.ballot
	id := n.id
	n.mu.Unlock()

	votes := 1
	respCh := make(chan bool, len(n.peers))

	for _, peerId := range n.peers {
		targetNode := nodeList.get(peerId)
		go func(node *Node) {
			resp := node.handleCommit(CommitReq{
				Ballot:   ballot,
				LeaderID: id,
				Value:    value,
			})
			respCh <- resp.OK
		}(targetNode)
	}

	for i := 0; i < len(n.peers); i++ {
		if <-respCh {
			votes++
		}
	}

	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1
	return votes >= majority
}

func (n *Node) handleCommit(req CommitReq) CommitResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Ballot >= n.ballot {
		n.ballot = req.Ballot
		n.committedVal = req.Value
		return CommitResp{
			Ballot: n.ballot,
			OK:     true,
		}
	}
	return CommitResp{
		Ballot: n.ballot,
		OK:     false,
	}
}

func (n *Node) sendHeartbeat(nodeList *NodeList) {
	n.mu.Lock()
	ballot := n.ballot
	id := n.id
	n.mu.Unlock()

	for _, peerId := range n.peers {
		peerNode := nodeList.get(peerId)
		go peerNode.handleHeartbeat(HeartbeatReq{
			Ballot:   ballot,
			LeaderID: id,
		})
	}
}

func (n *Node) handleHeartbeat(req HeartbeatReq) HeartbeatResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Ballot >= n.ballot {
		n.ballot = req.Ballot
		n.role = FOLLOWER
		n.votedFor = req.LeaderID
		n.leaseExpiry = time.Now().Add(100 * time.Millisecond)

		fmt.Printf("[%s] got heartbeat from %s ballot %d\n", n.id, req.LeaderID, req.Ballot)

		n.resetElectionTimer()

		return HeartbeatResp{Ballot: n.ballot, OK: true}
	}
	return HeartbeatResp{Ballot: n.ballot, OK: false}
}

func (n *Node) runHeartbeat(nodeList *NodeList) {
	for {
		n.mu.Lock()
		if n.role != LEADER {
			n.mu.Unlock()
			return
		}
		n.leaseExpiry = time.Now().Add(100 * time.Millisecond)
		n.mu.Unlock()

		n.sendHeartbeat(nodeList)
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) SendPreVote(nodeList *NodeList) bool {
	n.mu.Lock()
	nextBallot := n.ballot + 1
	id := n.id
	n.mu.Unlock()

	value := 0
	respCh := make(chan bool, len(n.peers))

	for _, peerId := range n.peers {
		targetNode := nodeList.get(peerId)
		go func(node *Node) {
			resp := node.handlePreVote(PreVoteReq{
				Ballot:   nextBallot,
				LeaderID: id,
			})
			respCh <- resp == 1
		}(targetNode)
	}

	for i := 0; i < len(n.peers); i++ {
		if <-respCh {
			value++
		}
	}

	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1
	return (value + 1) >= majority
}

func (n *Node) handlePreVote(req PreVoteReq) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	if time.Now().Before(n.leaseExpiry) {
		return 0
	}

	if req.Ballot > n.ballot && n.role != LEADER {
		return 1
	}
	return 0
}

func (n *Node) runElectionTimer(nodeList *NodeList) {
	for {
		timeout := time.Duration(rand.IntN(151)+150) * time.Millisecond
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()
			if role != LEADER {
				n.startElection(nodeList)
			}
		case <-n.resetCh:
		}
	}
}

func (n *Node) resetElectionTimer() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

func (n *Node) startElection(nodeList *NodeList) {
	if !n.SendPreVote(nodeList) {
		return
	}

	n.mu.Lock()
	n.ballot++
	n.role = CANDIDATE
	n.votedFor = n.id
	n.mu.Unlock()

	votes, recoveredValue := n.sendPrepare(nodeList)
	if recoveredValue == "" {
		recoveredValue = "no-op"
	}

	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1

	n.mu.Lock()
	defer n.mu.Unlock()
	if votes >= majority && n.role == CANDIDATE {
		n.role = LEADER
		n.electionAttempts = 0
		n.leaseExpiry = time.Now().Add(100 * time.Millisecond)
		fmt.Printf("[%s] became LEADER at ballot %d\n", n.id, n.ballot)
		go n.runHeartbeat(nodeList)
		go func() {
			if n.sendAccept(recoveredValue, nodeList) {
				n.sendCommit(recoveredValue, nodeList)
			}
		}()
	} else if n.role == CANDIDATE {
		n.electionAttempts++
		backoff := time.Duration(150<<n.electionAttempts) * time.Millisecond
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		fmt.Printf("[%s] election FAILED, back to follower (backoff %v)\n", n.id, backoff)
		n.role = FOLLOWER
		time.Sleep(backoff)
	}
}

func main() {
	node1Id := "1"
	node2Id := "2"
	node3Id := "3"
	node4Id := "4"
	node5Id := "5"

	nodeList := &NodeList{list: make(map[string]*Node)}

	node1 := NewNode(node1Id, []string{node2Id, node3Id, node4Id, node5Id}, nodeList)
	node2 := NewNode(node2Id, []string{node1Id, node3Id, node4Id, node5Id}, nodeList)
	node3 := NewNode(node3Id, []string{node1Id, node2Id, node4Id, node5Id}, nodeList)
	node4 := NewNode(node4Id, []string{node1Id, node2Id, node3Id, node5Id}, nodeList)
	node5 := NewNode(node5Id, []string{node1Id, node2Id, node3Id, node4Id}, nodeList)

	go node1.runElectionTimer(nodeList)
	go node2.runElectionTimer(nodeList)
	go node3.runElectionTimer(nodeList)
	go node4.runElectionTimer(nodeList)
	go node5.runElectionTimer(nodeList)

	select {}
}
