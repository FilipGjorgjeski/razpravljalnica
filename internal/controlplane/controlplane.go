package controlplane

import (
	"context"
	"flag"
	"net"
	"strings"
	"sync"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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
	fs.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", 3*time.Second, "node failure timeout")
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

	watchMu     sync.Mutex
	nextWatchID int64
	watchers    map[int64]chan *pb.GetClusterStateResponse
}

func New(cfg Config) *Server {
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 3 * time.Second
	}
	return &Server{cfg: cfg, nodes: map[string]*nodeRuntime{}, watchers: map[int64]chan *pb.GetClusterStateResponse{}}
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
	s.mu.Unlock()

	chain, head, tail, ver, changed := s.computeChain(now)
	role, pred, succ := roleAndNeighbors(id, chain)
	if changed {
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
		if !alive(rt) {
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
	}
	return chain, head, tail, s.configVer, changed
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
