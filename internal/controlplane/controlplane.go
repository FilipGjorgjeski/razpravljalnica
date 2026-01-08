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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	mu        sync.RWMutex
	nodes     map[string]*nodeRuntime
	configVer int64
	lastSig   string
	pending   map[string]*pb.NodeInfo
	alive     map[string]bool

	watchMu     sync.Mutex
	nextWatchID int64
	watchers    map[int64]chan *pb.GetClusterStateResponse
}

func New(cfg Config) *Server {
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 3 * time.Second
	}
	return &Server{cfg: cfg, nodes: map[string]*nodeRuntime{}, pending: map[string]*pb.NodeInfo{}, watchers: map[int64]chan *pb.GetClusterStateResponse{}, alive: map[string]bool{}}
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

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id := strings.TrimSpace(req.GetNodeId())
	addr := strings.TrimSpace(req.GetAddress())
	if id == "" || addr == "" {
		return &pb.HeartbeatResponse{Role: pb.ChainRole_ROLE_UNKNOWN}, nil
	}

	now := time.Now().UTC()

	s.mu.Lock()
	rt := s.nodes[id]
	if rt == nil {
		rt = &nodeRuntime{}
		s.nodes[id] = rt
	}
	rt.info = &pb.NodeInfo{NodeId: id, Address: addr}
	rt.status = req.GetStatus()
	rt.lastSeenAt = now

	log.Printf("[control-plane] heartbeat node=%s addr=%s applied=%d committed=%d", id, addr, rt.status.GetLastApplied(), rt.status.GetLastCommitted())

	// Promote any pending nodes that have caught up to the current tail's committed seq.
	changed := s.promotePendingLocked(now)
	s.mu.Unlock()

	chain, head, tail, ver, computeChanged := s.computeChain(now)
	changed = changed || computeChanged
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

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, existing := range s.cfg.ChainOrder {
		if existing == id {
			return nil, status.Error(codes.AlreadyExists, "node already in chain order")
		}
	}
	if _, ok := s.pending[id]; ok {
		return nil, status.Error(codes.AlreadyExists, "node already pending")
	}

	s.pending[id] = &pb.NodeInfo{NodeId: id, Address: addr}
	log.Printf("[control-plane] add pending node=%s addr=%s", id, addr)
	// Remember the address so the node runtime is discoverable before its first heartbeat.
	rt := s.nodes[id]
	if rt == nil {
		rt = &nodeRuntime{}
		s.nodes[id] = rt
	}
	rt.info = &pb.NodeInfo{NodeId: id, Address: addr}

	// If the node is already alive and caught up, promote immediately.
	now := time.Now().UTC()
	promoted := s.promotePendingLocked(now)
	if promoted {
		log.Printf("[control-plane] node=%s promoted immediately on add", id)
	}

	return s.currentTailLocked(), nil
}

// ActivateNode appends a pending node to the chain order after it has synced.
func (s *Server) ActivateNode(ctx context.Context, req *pb.ActivateNodeRequest) (*pb.GetClusterStateResponse, error) {
	id := strings.TrimSpace(req.GetNodeId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id required")
	}

	s.mu.Lock()
	info, ok := s.pending[id]
	if !ok {
		// If already in chain, treat as idempotent success.
		for _, existing := range s.cfg.ChainOrder {
			if existing == id {
				s.mu.Unlock()
				return s.GetClusterState(ctx, &emptypb.Empty{})
			}
		}
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "node not pending")
	}
	s.cfg.ChainOrder = append(s.cfg.ChainOrder, id)
	delete(s.pending, id)
	// Ensure runtime exists and is considered alive immediately after activation.
	rt := s.nodes[id]
	if rt == nil {
		rt = &nodeRuntime{}
		s.nodes[id] = rt
	}
	if info != nil {
		rt.info = info
	}
	rt.lastSeenAt = time.Now().UTC()
	s.mu.Unlock()

	now := time.Now().UTC()
	chain, head, tail, ver, changed := s.computeChain(now)
	resp := &pb.GetClusterStateResponse{Head: head, Tail: tail, Chain: chain, ConfigVersion: ver}
	if changed {
		log.Printf("[control-plane] activate node=%s ver=%d chain=%v", id, ver, ids(chain))
		s.broadcastClusterState(resp)
	}

	// Ensure we have the latest address from pending info if the node hasn't heartbeated yet.
	if info != nil {
		for i := range chain {
			if chain[i] != nil && chain[i].GetNodeId() == info.NodeId && chain[i].GetAddress() == "" {
				chain[i].Address = info.Address
			}
		}
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
