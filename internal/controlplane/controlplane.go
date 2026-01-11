package controlplane

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Config struct {
	// ChainOrder is the desired chain order: head..tail by node_id.
	// Nodes not listed here will be ignored.
	ChainOrder []string

	// HeartbeatTimeout marks nodes as failed if they don't heartbeat in time.
	HeartbeatTimeout time.Duration
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", 6*time.Second, "node failure timeout")
	fs.Func("chain", "comma-separated node IDs in chain order (head..tail)", func(v string) error {
		v = strings.TrimSpace(v)
		if v == "" {
			c.ChainOrder = nil
			return nil
		}
		parts := strings.Split(v, ",")
		order := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			order = append(order, p)
		}
		c.ChainOrder = order
		return nil
	})
}

type nodeRuntime struct {
	info       *pb.NodeInfo
	status     *pb.NodeStatus
	lastSeenAt time.Time
}

type Server struct {
	pb.UnimplementedControlPlaneServer

	cfg Config

	// Control plane state
	mu        sync.RWMutex
	nodes     map[string]*nodeRuntime
	configVer int64
	lastSig   string
	pending   map[string]*pb.NodeInfo
	alive     map[string]bool

	// Watchers
	watchMu     sync.Mutex
	nextWatchID int64
	watchers    map[int64]chan *pb.GetClusterStateResponse

	// RAFT state
	raftConfig  RaftConfig
	raft        *raft.Raft
	fsm         *controlPlaneFSM
	transport   *raft.NetworkTransport
	logStore    raft.LogStore
	stableStore raft.StableStore
	snapStore   raft.SnapshotStore

	grpcListenAddr string
	grpcAddrsMu    sync.RWMutex
	grpcAddrs      map[string]string
}

func New(cfg Config) *Server {
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 3 * time.Second
	}
	s := &Server{
		cfg:       cfg,
		nodes:     map[string]*nodeRuntime{},
		pending:   map[string]*pb.NodeInfo{},
		watchers:  map[int64]chan *pb.GetClusterStateResponse{},
		alive:     map[string]bool{},
		grpcAddrs: map[string]string{},
	}

	s.fsm = &controlPlaneFSM{server: s}
	return s
}

func NewWithRAFT(cfg Config, raftCfg RaftConfig, listenAddress string) *Server {
	s := New(cfg)
	s.raftConfig = raftCfg
	s.grpcListenAddr = listenAddress
	return s
}

func (s *Server) Register(gs *grpc.Server) {
	pb.RegisterControlPlaneServer(gs, s)
	reflection.Register(gs)
}

func ListenAndServe(listenAddr string, srv *Server) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	return gs.Serve(lis)
}

// Apply command through RAFT if enabled
func (s *Server) raftApply(cmd Command, timeout time.Duration) (interface{}, error) {
	if s.raft == nil {
		data, err := cmd.Marshal()
		if err != nil {
			return nil, err
		}
		return s.fsm.Apply(&raft.Log{Data: data}), nil
	}

	if !s.isRAFTLeader() {
		leader := s.raft.Leader()
		if leader == "" {
			return nil, status.Error(codes.Unavailable, "no leader elected")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader, leader is %s", leader)
	}

	data, err := cmd.Marshal()
	if err != nil {
		return nil, err
	}

	future := s.raft.Apply(data, timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	return future.Response(), nil
}

func (s *Server) isRAFTLeader() bool {
	if s.raft == nil {
		return true
	}
	return s.raft.State() == raft.Leader
}

func (s *Server) getLeaderNodeID() (string, error) {
	leaderRaftAddr := s.raft.Leader()
	if leaderRaftAddr == "" {
		return "", status.Error(codes.Unavailable, "no leader elected")
	}

	configFuture := s.raft.GetConfiguration()
	if configFuture.Error() != nil {
		return "", configFuture.Error()
	}

	var leaderID string
	for _, server := range configFuture.Configuration().Servers {
		if server.Address == leaderRaftAddr {
			leaderID = string(server.ID)
			break
		}
	}

	if leaderID == "" {
		return "", status.Error(codes.Internal, "leader ID not found")
	}

	return leaderID, nil
}

func (s *Server) getLeaderGrpcAddr() (string, error) {
	if s.raft == nil {
		return "", nil
	}

	if s.isRAFTLeader() {
		return "", nil
	}

	leaderID, err := s.getLeaderNodeID()
	if err != nil {
		return "", err
	}

	s.grpcAddrsMu.RLock()
	grpcAddr := s.grpcAddrs[leaderID]
	s.grpcAddrsMu.RUnlock()

	if grpcAddr == "" {
		return "", status.Errorf(codes.Internal, "gRPC address not found for leader %s", leaderID)
	}

	return grpcAddr, nil
}

func forwardToLeader[T any](s *Server, originalErr error, handler func(pb.ControlPlaneClient) (T, error)) (T, error) {
	var empty T

	leaderAddr, lookupErr := s.getLeaderGrpcAddr()
	if lookupErr != nil || leaderAddr == "" {
		log.Printf("[raft]: forwarding to leader failed: %s", lookupErr)
		return empty, originalErr
	}

	log.Printf("[raft]: forwarding request to leader at %s", leaderAddr)

	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return empty, err
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	return handler(client)
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id := strings.TrimSpace(req.GetNodeId())
	addr := strings.TrimSpace(req.GetAddress())
	if id == "" || addr == "" {
		return &pb.HeartbeatResponse{Role: pb.ChainRole_ROLE_UNKNOWN}, nil
	}

	cmd, err := NewHeartbeatCommand(id, addr, req.GetStatus())
	if err != nil {
		return nil, err
	}

	_, err = s.raftApply(cmd, 3*time.Second)
	if err != nil {
		return forwardToLeader(s, err, func(client pb.ControlPlaneClient) (*pb.HeartbeatResponse, error) {
			return client.Heartbeat(ctx, req)
		})
	}

	now := time.Now().UTC()
	chain, head, tail, ver, changed := s.computeChain(now)
	role, pred, succ := roleAndNeighbors(id, chain)
	if changed {
		log.Printf("[control-plane] cluster change ver=%d head=%s tail=%s chain=%v", ver, safeNodeID(head), safeNodeID(tail), ids(chain))
		s.broadcastClusterState(&pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver})
	}

	return &pb.HeartbeatResponse{
		ConfigVersion: ver,
		Role:          role,
		Head:          head,
		Tail:          tail,
		Predecessor:   pred,
		Successor:     succ,
		Chain:         chain,
	}, nil
}

func (s *Server) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	now := time.Now().UTC()
	chain, head, tail, ver, changed := s.computeChain(now)
	if changed {
		s.broadcastClusterState(&pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver})
	}
	return &pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver}, nil
}

// AddNode records a node as pending so it can bootstrap from the current tail.
func (s *Server) AddNode(ctx context.Context, req *pb.AddNodeRequest) (*pb.AddNodeResponse, error) {
	id := strings.TrimSpace(req.GetNodeId())
	addr := strings.TrimSpace(req.GetAddress())
	if id == "" || addr == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and address required")
	}

	cmd, err := NewAddNodeCommand(id, addr)
	if err != nil {
		return nil, err
	}

	_, err = s.raftApply(cmd, 5*time.Second)
	if err != nil {
		return forwardToLeader(s, err, func(client pb.ControlPlaneClient) (*pb.AddNodeResponse, error) {
			return client.AddNode(ctx, req)
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTailLocked(), nil
}

// ActivateNode appends a pending node to the chain order after it has synced.
func (s *Server) ActivateNode(ctx context.Context, req *pb.ActivateNodeRequest) (*pb.GetClusterStateResponse, error) {
	id := strings.TrimSpace(req.GetNodeId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id required")
	}

	cmd, err := NewActivateNodeCommand(id)
	if err != nil {
		return nil, err
	}

	_, err = s.raftApply(cmd, 5*time.Second)
	if err != nil {
		return forwardToLeader(s, err, func(client pb.ControlPlaneClient) (*pb.GetClusterStateResponse, error) {
			return client.ActivateNode(ctx, req)
		})
	}

	now := time.Now()
	chain, head, tail, ver, changed := s.computeChain(now)
	resp := &pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver}
	if changed {
		log.Printf("[control-plane] activate node=%s ver=%d chain=%v", id, ver, ids(chain))
		s.broadcastClusterState(resp)
	}

	return resp, nil
}

// currentTailLocked returns the current tail info while holding s.mu.
func (s *Server) currentTailLocked() *pb.AddNodeResponse {
	now := time.Now().UTC()

	alive := func(n *nodeRuntime) bool {
		if n == nil || n.info == nil {
			return false
		}
		return now.Sub(n.lastSeenAt) <= s.cfg.HeartbeatTimeout
	}

	var tailInfo *pb.NodeInfo
	for i := len(s.cfg.ChainOrder) - 1; i >= 0; i-- {
		id := s.cfg.ChainOrder[i]
		rt := s.nodes[id]
		if !alive(rt) {
			continue
		}
		tailInfo = rt.info
		break
	}
	return &pb.AddNodeResponse{CurrentTail: tailInfo}
}

func (s *Server) WatchClusterState(req *pb.WatchClusterStateRequest, stream pb.ControlPlane_WatchClusterStateServer) error {
	// Register watcher.
	sch := make(chan *pb.GetClusterStateResponse, 1)
	s.watchMu.Lock()
	s.nextWatchID++
	id := s.nextWatchID
	s.watchers[id] = sch
	s.watchMu.Unlock()
	defer func() {
		s.watchMu.Lock()
		delete(s.watchers, id)
		s.watchMu.Unlock()
	}()

	// Send current state immediately.
	now := time.Now().UTC()
	chain, head, tail, ver, changed := s.computeChain(now)
	cur := &pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver}
	if changed {
		s.broadcastClusterState(cur)
	}
	if err := stream.Send(cur); err != nil {
		return err
	}

	lastSeen := req.GetLastSeenConfigVersion()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case upd := <-sch:
			if upd == nil {
				continue
			}
			if lastSeen > 0 && upd.GetConfigVersion() <= lastSeen {
				continue
			}
			if err := stream.Send(upd); err != nil {
				return err
			}
			lastSeen = upd.GetConfigVersion()
		}
	}
}

func (s *Server) broadcastClusterState(state *pb.GetClusterStateResponse) {
	if state == nil {
		return
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, ch := range s.watchers {
		// Non-blocking: if a client is slow, drop intermediate updates.
		select {
		case ch <- state:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- state:
			default:
			}
		}
	}
}

func (s *Server) computeChain(now time.Time) (chain []*pb.NodeInfo, head *pb.NodeInfo, tail *pb.NodeInfo, ver int64, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	alive := func(n *nodeRuntime) bool {
		if n == nil || n.info == nil {
			return false
		}
		return now.Sub(n.lastSeenAt) <= s.cfg.HeartbeatTimeout
	}

	chain = make([]*pb.NodeInfo, 0, len(s.cfg.ChainOrder))
	for _, id := range s.cfg.ChainOrder {
		rt := s.nodes[id]
		isAlive := alive(rt)
		prev := s.alive[id]
		if isAlive != prev {
			if isAlive {
				log.Printf("[control-plane] node alive node=%s", id)
			} else {
				log.Printf("[control-plane] node dead (heartbeat timeout) node=%s", id)
			}
			s.alive[id] = isAlive
		}
		if !isAlive {
			continue
		}
		chain = append(chain, &pb.NodeInfo{NodeId: rt.info.NodeId, Address: rt.info.Address})
	}
	if len(chain) > 0 {
		head = chain[0]
		tail = chain[len(chain)-1]
	}

	parts := make([]string, 0, len(chain))
	for _, n := range chain {
		parts = append(parts, n.NodeId+"@"+n.Address)
	}
	sig := strings.Join(parts, ";")
	if sig != s.lastSig {
		s.configVer++
		s.lastSig = sig
		changed = true
		log.Printf("[control-plane] config change ver=%d head=%s tail=%s chain=%v", s.configVer, safeNodeID(head), safeNodeID(tail), ids(chain))
	}
	return chain, head, tail, s.configVer, changed
}

func ids(chain []*pb.NodeInfo) []string {
	res := make([]string, 0, len(chain))
	for _, n := range chain {
		if n == nil {
			continue
		}
		res = append(res, n.NodeId+"@"+n.Address)
	}
	return res
}

func safeNodeID(n *pb.NodeInfo) string {
	if n == nil {
		return ""
	}
	return n.NodeId
}

// promotePendingLocked appends any pending nodes that have caught up to the current tail.
// Caller must hold s.mu.
func (s *Server) promotePendingLocked(now time.Time) bool {
	if len(s.pending) == 0 {
		return false
	}

	alive := func(n *nodeRuntime) bool {
		if n == nil || n.info == nil {
			return false
		}
		return now.Sub(n.lastSeenAt) <= s.cfg.HeartbeatTimeout
	}

	promoted := false
	for id, info := range s.pending {
		rt := s.nodes[id]
		if !alive(rt) {
			continue
		}
		// Append to desired chain order if not already present.
		exists := false
		for _, existing := range s.cfg.ChainOrder {
			if existing == id {
				exists = true
				break
			}
		}
		if !exists {
			s.cfg.ChainOrder = append(s.cfg.ChainOrder, id)
		}
		// Ensure address is retained when the node later becomes active.
		rt.info = info
		delete(s.pending, id)
		log.Printf("[control-plane] auto-promote node=%s", id)
		promoted = true
	}
	return promoted
}

func roleAndNeighbors(nodeID string, chain []*pb.NodeInfo) (role pb.ChainRole, pred *pb.NodeInfo, succ *pb.NodeInfo) {
	idx := -1
	for i, n := range chain {
		if n.NodeId == nodeID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return pb.ChainRole_ROLE_UNKNOWN, nil, nil
	}
	if idx == 0 {
		role = pb.ChainRole_ROLE_HEAD
	} else if idx == len(chain)-1 {
		role = pb.ChainRole_ROLE_TAIL
	} else {
		role = pb.ChainRole_ROLE_MIDDLE
	}
	if idx > 0 {
		pred = chain[idx-1]
	}
	if idx < len(chain)-1 {
		succ = chain[idx+1]
	}
	return role, pred, succ
}
