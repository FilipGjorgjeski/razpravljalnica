// No changes needed as s.logf is not present in the original code.
package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ChainNode struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedReplicationServer

	store *storage.Storage
	hub   *Hub

	nodeID        string
	advertiseAddr string
	listenAddr    string

	controlPlaneAddr string
	heartbeatEvery   time.Duration

	mu      sync.RWMutex
	role    pb.ChainRole
	head    *pb.NodeInfo
	tail    *pb.NodeInfo
	pred    *pb.NodeInfo
	succ    *pb.NodeInfo
	chain   []*pb.NodeInfo
	configV int64

	subAssignIdx int

	forwardCh chan *pb.LogEntry
	ackCh     chan int64

	waitMu   sync.Mutex
	waitAcks map[int64]chan struct{}

	broadcastMu     sync.Mutex
	lastBroadcasted int64
}

func (n *ChainNode) logf(format string, args ...interface{}) {
	log.Printf("[node %s] "+format, append([]interface{}{n.nodeID}, args...)...)
}

func nodeLabel(n *pb.NodeInfo) string {
	if n == nil {
		return ""
	}
	return n.GetNodeId() + "@" + n.GetAddress()
}

func labels(nodes []*pb.NodeInfo) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		out = append(out, nodeLabel(n))
	}
	return out
}

func sameNode(a, b *pb.NodeInfo) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.GetNodeId() == b.GetNodeId() && a.GetAddress() == b.GetAddress()
}

func NewChainNode(store *storage.Storage, hub *Hub, nodeID, listenAddr, advertiseAddr, controlPlaneAddr string) *ChainNode {
	if hub == nil {
		hub = NewHub()
	}
	return &ChainNode{
		store:            store,
		hub:              hub,
		nodeID:           nodeID,
		listenAddr:       listenAddr,
		advertiseAddr:    advertiseAddr,
		controlPlaneAddr: controlPlaneAddr,
		heartbeatEvery:   2000 * time.Millisecond,
		forwardCh:        make(chan *pb.LogEntry, 1024),
		ackCh:            make(chan int64, 1024),
		waitAcks:         map[int64]chan struct{}{},
		role:             pb.ChainRole_ROLE_UNKNOWN,
	}
}

func (n *ChainNode) Register(gs *grpc.Server) {
	pb.RegisterMessageBoardServer(gs, n)
	pb.RegisterReplicationServer(gs, n)
	reflection.Register(gs)
}

func (n *ChainNode) ListenAndServe() error {
	lis, err := net.Listen("tcp", n.listenAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	n.Register(gs)

	// Background control-plane + replication loops.
	n.Start()

	return gs.Serve(lis)
}

func (n *ChainNode) Start() {
	if n.controlPlaneAddr != "" {
		go n.heartbeatLoop()
	}
	go n.forwardLoop()
	go n.ackForwardLoop()
}

func (n *ChainNode) heartbeatLoop() {
	var conn *grpc.ClientConn
	var client pb.ControlPlaneClient

	dial := func() {
		for {
			c, err := grpc.NewClient(n.controlPlaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				conn = c
				client = pb.NewControlPlaneClient(conn)
				n.logf("control-plane connected addr=%s", n.controlPlaneAddr)
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	dial()

	t := time.NewTicker(n.heartbeatEvery)
	defer t.Stop()

	for range t.C {
		applied, committed := n.store.ChainStatus()
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
			NodeId:  n.nodeID,
			Address: n.advertiseAddr,
			Status:  &pb.NodeStatus{LastApplied: applied, LastCommitted: committed},
		})
		cancel()
		if err != nil {
			n.logf("heartbeat failed: %v", err)
			// Re-dial on failure.
			_ = conn.Close()
			dial()
			continue
		}

		n.applyAssignment(resp)
	}
}

func (n *ChainNode) applyAssignment(resp *pb.HeartbeatResponse) {
	n.mu.Lock()
	oldRole := n.role
	oldHead := n.head
	oldTail := n.tail
	oldPred := n.pred
	oldSucc := n.succ

	n.configV = resp.GetConfigVersion()
	n.role = resp.GetRole()
	n.head = resp.GetHead()
	n.tail = resp.GetTail()
	n.pred = resp.GetPredecessor()
	n.succ = resp.GetSuccessor()
	chain := resp.GetChain()
	if len(chain) > 0 {
		n.chain = make([]*pb.NodeInfo, len(chain))
		for i := range chain {
			if chain[i] != nil {
				n.chain[i] = &pb.NodeInfo{NodeId: chain[i].GetNodeId(), Address: chain[i].GetAddress()}
			}
		}
	}

	newPred := n.pred
	newSucc := n.succ
	n.mu.Unlock()

	if oldRole != n.role || !sameNode(oldHead, n.head) || !sameNode(oldTail, n.tail) || !sameNode(oldPred, newPred) || !sameNode(oldSucc, newSucc) {
		n.logf("assignment ver=%d role=%s head=%s tail=%s pred=%s succ=%s", n.configV, n.role.String(), nodeLabel(n.head), nodeLabel(n.tail), nodeLabel(newPred), nodeLabel(newSucc))
	}

	// Resync on topology change.
	if (oldPred == nil) != (newPred == nil) || (oldPred != nil && newPred != nil && oldPred.Address != newPred.Address) {
		go n.pullMissingFromPredecessor()
		go n.pushCommittedAckUpstream()
	}
	if (oldSucc == nil) != (newSucc == nil) || (oldSucc != nil && newSucc != nil && oldSucc.Address != newSucc.Address) {
		go n.pushMissingToSuccessor()
	}
}

func (n *ChainNode) getNeighbors() (role pb.ChainRole, head, tail, pred, succ *pb.NodeInfo) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role, n.head, n.tail, n.pred, n.succ
}

func (n *ChainNode) forwardLoop() {
	var conn *grpc.ClientConn
	var client pb.ReplicationClient
	var connectedTo string

	for entry := range n.forwardCh {
		_, _, _, _, succ := n.getNeighbors()
		if succ == nil || succ.Address == "" {
			continue
		}
		if connectedTo != succ.Address {
			if conn != nil {
				_ = conn.Close()
				conn = nil
				client = nil
				connectedTo = ""
			}
			c, err := grpc.NewClient(succ.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				time.Sleep(200 * time.Millisecond)
				n.forwardCh <- entry
				continue
			}
			conn = c
			client = pb.NewReplicationClient(conn)
			connectedTo = succ.Address
			n.logf("replication connected downstream=%s", succ.Address)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.Replicate(ctx, &pb.ReplicateRequest{Entry: entry})
		cancel()
		if err != nil {
			// Retry later.
			time.Sleep(200 * time.Millisecond)
			n.logf("replicate retry seq=%d downstream=%s err=%v", entry.GetSequenceNumber(), succ.Address, err)
			n.forwardCh <- entry
		}
	}
}

func (n *ChainNode) ackForwardLoop() {
	var conn *grpc.ClientConn
	var client pb.ReplicationClient
	var connectedTo string

	for seq := range n.ackCh {
		_, _, _, pred, _ := n.getNeighbors()
		if pred == nil || pred.Address == "" {
			continue
		}
		if connectedTo != pred.Address {
			if conn != nil {
				_ = conn.Close()
				conn = nil
				client = nil
				connectedTo = ""
			}
			c, err := grpc.NewClient(pred.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				time.Sleep(200 * time.Millisecond)
				n.ackCh <- seq
				continue
			}
			conn = c
			client = pb.NewReplicationClient(conn)
			connectedTo = pred.Address
			n.logf("ack connected upstream=%s", pred.Address)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.Ack(ctx, &pb.AckRequest{SequenceNumber: seq})
		cancel()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			n.logf("ack retry seq=%d upstream=%s err=%v", seq, pred.Address, err)
			n.ackCh <- seq
		}
	}
}

func (n *ChainNode) pullMissingFromPredecessor() {
	_, _, _, pred, _ := n.getNeighbors()
	if pred == nil || pred.Address == "" {
		return
	}
	n.logf("pull missing from predecessor=%s", nodeLabel(pred))
	applied := n.store.ChainLastAppliedSeq()

	conn, err := grpc.NewClient(pred.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	c := pb.NewReplicationClient(conn)

	pulled := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := c.GetEntries(ctx, &pb.GetEntriesRequest{FromSequenceNumber: applied + 1, Limit: 256})
		cancel()
		if err != nil {
			return
		}
		entries := resp.GetEntries()
		if len(entries) == 0 {
			break
		}
		for _, pe := range entries {
			se := pbToStorageEntry(pe)
			_, err := n.store.ChainApplyReplicatedEntry(se)
			if err != nil {
				return
			}
			applied = se.Seq
			pulled++
		}
	}

	// If we are tail (no successor), mark committed up to what we pulled so heartbeats reflect it.
	role, _, _, _, succ := n.getNeighbors()
	if role == pb.ChainRole_ROLE_TAIL || succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(applied)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(applied)
	}

	n.logf("pulled %d entries from predecessor=%s", pulled, nodeLabel(pred))
}

func (n *ChainNode) pushMissingToSuccessor() {
	_, _, _, _, succ := n.getNeighbors()
	if succ == nil || succ.Address == "" {
		return
	}
	n.logf("push missing to successor=%s", nodeLabel(succ))

	conn, err := grpc.NewClient(succ.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	c := pb.NewReplicationClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	st, err := c.GetStatus(ctx, &emptypb.Empty{})
	cancel()
	if err != nil {
		return
	}
	from := st.GetLastApplied() + 1
	entries := n.store.ChainEntriesFrom(from, 0)
	pushed := 0
	for i := range entries {
		pe := storageToPBEntry(entries[i])
		n.forwardCh <- pe
		pushed++
	}
	n.logf("queued %d missing entries to successor=%s", pushed, nodeLabel(succ))
}

func (n *ChainNode) pushCommittedAckUpstream() {
	_, committed := n.store.ChainStatus()
	if committed <= 0 {
		return
	}
	n.ackCh <- committed
}

func randomRequestID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func randomSubscriptionToken() string {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func label(n *pb.NodeInfo) string {
	if n == nil {
		return ""
	}
	return n.NodeId + "@" + n.Address
}

func (n *ChainNode) chooseSubscriptionNode() *pb.NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()

	candidates := make([]*pb.NodeInfo, 0, len(n.chain))
	for _, node := range n.chain {
		if node == nil || node.GetAddress() == "" {
			continue
		}
		if node.GetNodeId() == n.nodeID {
			continue
		}
		candidates = append(candidates, &pb.NodeInfo{NodeId: node.GetNodeId(), Address: node.GetAddress()})
	}
	if len(candidates) == 0 {
		return &pb.NodeInfo{NodeId: n.nodeID, Address: n.advertiseAddr}
	}
	idx := n.subAssignIdx % len(candidates)
	n.subAssignIdx++
	return candidates[idx]
}

func (n *ChainNode) waitForCommit(ctx context.Context, seq int64) error {
	if seq <= 0 {
		return nil
	}
	// Fast-path: if already committed locally, don't wait.
	if n.store.ChainCommittedSeq() >= seq {
		return nil
	}
	ch := n.getWaiter(seq)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (n *ChainNode) getWaiter(seq int64) chan struct{} {
	n.waitMu.Lock()
	defer n.waitMu.Unlock()
	ch := n.waitAcks[seq]
	if ch == nil {
		ch = make(chan struct{})
		n.waitAcks[seq] = ch
		// If it got committed between the fast-path check and now, close immediately.
		if n.store.ChainCommittedSeq() >= seq {
			close(ch)
			delete(n.waitAcks, seq)
		}
	}
	return ch
}

func (n *ChainNode) signalCommitted(seq int64) {
	n.waitMu.Lock()
	for s, ch := range n.waitAcks {
		if s <= seq {
			close(ch)
			delete(n.waitAcks, s)
		}
	}
	n.waitMu.Unlock()
}

func (n *ChainNode) broadcastCommittedEvents(newEvents []storage.Event) {
	n.broadcastMu.Lock()
	defer n.broadcastMu.Unlock()
	for _, ev := range newEvents {
		if ev.SequenceNumber <= n.lastBroadcasted {
			continue
		}
		n.hub.Broadcast(ev)
		n.lastBroadcasted = ev.SequenceNumber
	}
}

// --- Replication service ---

func (n *ChainNode) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*emptypb.Empty, error) {
	entry := req.GetEntry()
	if entry == nil {
		return &emptypb.Empty{}, nil
	}

	se := pbToStorageEntry(entry)
	n.logf("replicate recv seq=%d role=%s", se.Seq, n.role.String())
	_, err := n.store.ChainApplyReplicatedEntry(se)
	if err != nil {
		if err == storage.ErrOutOfOrder {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, mapErr(err)
	}

	role, _, _, _, succ := n.getNeighbors()
	if role == pb.ChainRole_ROLE_TAIL || succ == nil {
		// Tail commits immediately.
		newEvents, _ := n.store.ChainCommitUpTo(se.Seq)
		n.broadcastCommittedEvents(newEvents)
		// Ack upstream.
		n.ackCh <- se.Seq
		n.logf("replicate tail commit seq=%d", se.Seq)
		return &emptypb.Empty{}, nil
	}

	// Forward downstream (pipelined).
	n.forwardCh <- entry
	n.logf("replicate forward seq=%d downstream=%s", se.Seq, nodeLabel(succ))
	return &emptypb.Empty{}, nil
}

func (n *ChainNode) Ack(ctx context.Context, req *pb.AckRequest) (*emptypb.Empty, error) {
	seq := req.GetSequenceNumber()
	if seq <= 0 {
		return &emptypb.Empty{}, nil
	}
	n.logf("ack recv seq=%d role=%s", seq, n.role.String())

	newEvents, _ := n.store.ChainCommitUpTo(seq)
	n.broadcastCommittedEvents(newEvents)

	role, _, _, pred, _ := n.getNeighbors()
	if role == pb.ChainRole_ROLE_HEAD || pred == nil {
		n.signalCommitted(seq)
		n.logf("ack done at head seq=%d", seq)
		return &emptypb.Empty{}, nil
	}

	// Forward ack upstream.
	n.ackCh <- seq
	n.logf("ack forward seq=%d upstream=%s", seq, nodeLabel(pred))
	return &emptypb.Empty{}, nil
}

func (n *ChainNode) GetEntries(ctx context.Context, req *pb.GetEntriesRequest) (*pb.GetEntriesResponse, error) {
	entries := n.store.ChainEntriesFrom(req.GetFromSequenceNumber(), req.GetLimit())
	res := make([]*pb.LogEntry, 0, len(entries))
	for i := range entries {
		res = append(res, storageToPBEntry(entries[i]))
	}
	return &pb.GetEntriesResponse{Entries: res}, nil
}

func (n *ChainNode) GetStatus(ctx context.Context, _ *emptypb.Empty) (*pb.NodeStatus, error) {
	applied, committed := n.store.ChainStatus()
	return &pb.NodeStatus{LastApplied: applied, LastCommitted: committed}, nil
}

// --- Data plane ---

func (n *ChainNode) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}

	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		if ef, ok := n.store.ChainEffect(rr.Seq); ok {
			return &pb.User{Id: ef.User.ID, Name: ef.User.Name}, nil
		}
		return &pb.User{Id: rr.UserID, Name: req.GetName()}, nil
	}

	entry, ef, err := n.store.ChainHeadCreateUser(req.GetName(), reqID, time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	pe := storageToPBEntry(entry)
	n.logf("append create_user seq=%d reqID=%s succ=%s", entry.Seq, reqID, nodeLabel(succ))
	if succ != nil {
		n.logf("forward create_user seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit create_user seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed create_user seq=%d", entry.Seq)
	}
	return &pb.User{Id: ef.User.ID, Name: ef.User.Name}, nil
}

func (n *ChainNode) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}
	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		if ef, ok := n.store.ChainEffect(rr.Seq); ok {
			return &pb.Topic{Id: ef.Topic.ID, Name: ef.Topic.Name}, nil
		}
		return &pb.Topic{Id: rr.TopicID, Name: req.GetName()}, nil
	}

	entry, ef, err := n.store.ChainHeadCreateTopic(req.GetName(), reqID, time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	n.logf("append create_topic seq=%d reqID=%s succ=%s", entry.Seq, reqID, nodeLabel(succ))
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("forward create_topic seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit create_topic seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed create_topic seq=%d", entry.Seq)
	}
	return &pb.Topic{Id: ef.Topic.ID, Name: ef.Topic.Name}, nil
}

func (n *ChainNode) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}
	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		if ef, ok := n.store.ChainEffect(rr.Seq); ok {
			return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, false), nil
		}
		return nil, status.Error(codes.Internal, "missing effect for request")
	}

	entry, ef, err := n.store.ChainHeadPostMessage(req.GetTopicId(), req.GetUserId(), req.GetText(), reqID, time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	n.logf("append post_message seq=%d reqID=%s topic=%d succ=%s", entry.Seq, reqID, req.GetTopicId(), nodeLabel(succ))
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("forward post_message seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit post_message seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed post_message seq=%d", entry.Seq)
	}
	return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, false), nil
}

func (n *ChainNode) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}
	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		if ef, ok := n.store.ChainEffect(rr.Seq); ok {
			return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, n.store.IsMessageLikedByUser(req.MessageId, req.UserId)), nil
		}
		return nil, status.Error(codes.Internal, "missing effect for request")
	}

	entry, ef, err := n.store.ChainHeadUpdateMessage(req.GetTopicId(), req.GetUserId(), req.GetMessageId(), req.GetText(), reqID, time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	n.logf("append update_message seq=%d reqID=%s msg=%d succ=%s", entry.Seq, reqID, req.GetMessageId(), nodeLabel(succ))
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("forward update_message seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit update_message seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed update_message seq=%d", entry.Seq)
	}
	return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, n.store.IsMessageLikedByUser(req.MessageId, req.UserId)), nil
}

func (n *ChainNode) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}
	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		return &emptypb.Empty{}, nil
	}

	entry, ef, err := n.store.ChainHeadDeleteMessage(req.GetTopicId(), req.GetUserId(), req.GetMessageId(), reqID, time.Now().UTC())
	_ = ef
	if err != nil {
		return nil, mapErr(err)
	}
	n.logf("append delete_message seq=%d reqID=%s msg=%d succ=%s", entry.Seq, reqID, req.GetMessageId(), nodeLabel(succ))
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("forward delete_message seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit delete_message seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed delete_message seq=%d", entry.Seq)
	}
	return &emptypb.Empty{}, nil
}

func (n *ChainNode) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		addr := ""
		if head != nil {
			addr = head.Address
		}
		return nil, status.Errorf(codes.FailedPrecondition, "writes go to head: %s", addr)
	}
	reqID := req.GetRequestId()
	if reqID == "" {
		reqID = randomRequestID()
	}
	if rr, ok := n.store.ChainLookupRequest(reqID); ok {
		if succ == nil {
			newEvents, _ := n.store.ChainCommitUpTo(rr.Seq)
			n.broadcastCommittedEvents(newEvents)
			n.signalCommitted(rr.Seq)
		} else {
			_ = n.waitForCommit(ctx, rr.Seq)
		}
		if ef, ok := n.store.ChainEffect(rr.Seq); ok {
			return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, true), nil
		}
		return nil, status.Error(codes.Internal, "missing effect for request")
	}

	entry, ef, err := n.store.ChainHeadLikeMessage(req.GetTopicId(), req.GetMessageId(), req.GetUserId(), reqID, time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	n.logf("append like_message seq=%d reqID=%s msg=%d succ=%s", entry.Seq, reqID, req.GetMessageId(), nodeLabel(succ))
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("forward like_message seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("commit like_message seq=%d (tail)", entry.Seq)
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("commit observed like_message seq=%d", entry.Seq)
	}
	return toProtoMessage(ef.Message, n.store.GetUser(ef.Message.UserID).Name, true), nil
}

func (n *ChainNode) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	role, _, tail, _, _ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_TAIL {
		n.logf("list_topics redirect_or_serve role=%s tail=%s", n.role.String(), nodeLabel(tail))
		// Serve locally if clean, else forward to tail.
		topics, dirty := n.store.ChainListTopicsCommitted()
		if dirty && tail != nil {
			conn, err := grpc.NewClient(tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer func() { _ = conn.Close() }()
				c := pb.NewMessageBoardClient(conn)
				return c.ListTopics(ctx, &emptypb.Empty{})
			}
		}
		res := make([]*pb.Topic, 0, len(topics))
		for _, t := range topics {
			res = append(res, &pb.Topic{Id: t.ID, Name: t.Name})
		}
		return &pb.ListTopicsResponse{Topics: res}, nil
	}
	// Tail serves directly.
	n.logf("list_topics tail role=%s", n.role.String())
	topics := n.store.ListTopics()
	res := make([]*pb.Topic, 0, len(topics))
	for _, t := range topics {
		res = append(res, &pb.Topic{Id: t.ID, Name: t.Name})
	}
	return &pb.ListTopicsResponse{Topics: res}, nil
}

func (n *ChainNode) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	role, _, tail, _, _ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_TAIL {
		n.logf("get_messages redirect_or_serve role=%s tail=%s topic=%d from=%d limit=%d", n.role.String(), nodeLabel(tail), req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
		msgs, dirty, err := n.store.ChainGetMessagesCommitted(req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
		if err != nil {
			return nil, mapErr(err)
		}
		if dirty && tail != nil {
			conn, err := grpc.NewClient(tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer func() { _ = conn.Close() }()
				c := pb.NewMessageBoardClient(conn)
				return c.GetMessages(ctx, req)
			}
		}
		res := make([]*pb.Message, 0, len(msgs))
		for _, m := range msgs {
			res = append(res, toProtoMessage(m, n.store.GetUser(m.UserID).Name, n.store.IsMessageLikedByUser(m.ID, req.UserId)))
		}
		return &pb.GetMessagesResponse{Messages: res}, nil
	}

	n.logf("get_messages tail role=%s topic=%d from=%d limit=%d", n.role.String(), req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
	msgs, err := n.store.GetMessages(req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
	if err != nil {
		return nil, mapErr(err)
	}
	res := make([]*pb.Message, 0, len(msgs))
	for _, m := range msgs {
		res = append(res, toProtoMessage(m, n.store.GetUser(m.UserID).Name, n.store.IsMessageLikedByUser(m.ID, req.UserId)))
	}
	return &pb.GetMessagesResponse{Messages: res}, nil
}

func (n *ChainNode) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	role, head, _, _, succ := n.getNeighbors()
	if role != pb.ChainRole_ROLE_HEAD {
		headAddr := ""
		if head != nil {
			headAddr = head.Address
		}
		if headAddr == "" {
			return nil, status.Error(codes.FailedPrecondition, "head unknown")
		}
		n.logf("forward GetSubscriptionNode to head=%s topics=%v", nodeLabel(head), req.GetTopicId())
		conn, err := grpc.NewClient(headAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "dial head: %v", err)
		}
		defer func() { _ = conn.Close() }()
		c := pb.NewMessageBoardClient(conn)
		return c.GetSubscriptionNode(ctx, req)
	}

	target := n.chooseSubscriptionNode()
	n.logf("subscription choose target=%s role=%s chain=%v", nodeLabel(target), n.role.String(), labels(n.chain))
	tok := randomSubscriptionToken()
	n.logf("assign subscription topics=%v target=%s token=%s", req.GetTopicId(), nodeLabel(target), tok)
	entry, err := n.store.ChainHeadRegisterSubscription(req.GetUserId(), req.GetTopicId(), tok, target.GetNodeId(), time.Now().UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	pe := storageToPBEntry(entry)
	if succ != nil {
		n.logf("replicate subscription seq=%d downstream=%s", entry.Seq, nodeLabel(succ))
		n.forwardCh <- pe
	}
	if succ == nil {
		newEvents, _ := n.store.ChainCommitUpTo(entry.Seq)
		n.broadcastCommittedEvents(newEvents)
		n.signalCommitted(entry.Seq)
		n.logf("subscription committed seq=%d target=%s (tail)", entry.Seq, nodeLabel(target))
	} else {
		if err := n.waitForCommit(ctx, entry.Seq); err != nil {
			return nil, err
		}
		n.logf("subscription committed seq=%d target=%s", entry.Seq, nodeLabel(target))
	}
	return &pb.SubscriptionNodeResponse{SubscribeToken: tok, Node: target}, nil
}

func (n *ChainNode) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	// Reuse the single-node implementation.
	// It stays consistent as long as we only broadcast committed events.
	if len(req.GetTopicId()) == 0 {
		return status.Error(codes.InvalidArgument, "topic_id required")
	}
	if req.GetUserId() <= 0 {
		return status.Error(codes.InvalidArgument, "user_id required")
	}
	if req.GetSubscribeToken() == "" {
		return status.Error(codes.Unauthenticated, "subscribe_token required")
	}
	if err := n.store.ValidateSubscriptionToken(req.GetSubscribeToken(), req.GetUserId(), req.GetTopicId(), n.nodeID); err != nil {
		return mapErr(err)
	}
	fromMessageID := req.GetFromMessageId()
	if fromMessageID < 0 {
		return status.Error(codes.InvalidArgument, "from_message_id must be >= 0")
	}

	n.logf("subscribe start user=%d topics=%v from=%d", req.GetUserId(), req.GetTopicId(), fromMessageID)

	_, ch, remove := n.hub.Add(req.GetTopicId())
	defer remove()

	watermark := n.store.CurrentSequence()
	backlog := n.store.EventsBetween(0, watermark)
	n.logf("subscribe backlog events=%d watermark=%d", len(backlog), watermark)

	topicSet := make(map[int64]struct{}, len(req.GetTopicId()))
	for _, tid := range req.GetTopicId() {
		topicSet[tid] = struct{}{}
	}

	var streamSeq int64 = 1
	send := func(op pb.OpType, m storage.Message, at time.Time) error {
		ev := &pb.MessageEvent{SequenceNumber: streamSeq, Op: op, Message: toProtoMessage(m, n.store.GetUser(m.UserID).Name, n.store.IsMessageLikedByUser(m.ID, req.UserId)), EventAt: timestamppb.New(at)}
		streamSeq++
		return stream.Send(ev)
	}
	for _, ev := range backlog {
		if _, ok := topicSet[ev.Message.TopicID]; !ok {
			continue
		}
		if ev.Message.ID < fromMessageID {
			continue
		}
		if err := send(toProtoOp(ev.Op), ev.Message, ev.EventAt); err != nil {
			return err
		}
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if ev.SequenceNumber <= watermark {
				continue
			}
			if ev.Message.ID < fromMessageID {
				continue
			}
			if err := send(toProtoOp(ev.Op), ev.Message, ev.EventAt); err != nil {
				return err
			}
		}
	}
}
