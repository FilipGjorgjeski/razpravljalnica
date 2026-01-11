package controlplane

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// RAFT implementation
// Using github.com/hashicorp/raft
// Using https://github.com/eatonphil/raft-example and https://medium.com/@filinvadim/hashicorp-raft-plug-n-play-ab0a04193315 as reference

type RaftConfig struct {
	// Determines if RAFT consensus is used
	Enabled bool

	// Unique identifier
	NodeID string

	// RAFT state storage
	DataDir string

	// Internal RAFT address
	BindAddr string

	// Should a new cluster be created?
	Bootstrap bool

	// Connect to cluster containing this node
	JoinAddr string
}

func (c *RaftConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.BoolVar(&c.Enabled, "raft-enabled", false, "enable RAFT for control plane")
	fs.StringVar(&c.NodeID, "raft-node-id", "", "unique ID for this control plane node (required if RAFT enabled)")
	fs.StringVar(&c.DataDir, "raft-data-dir", "./raft-data", "directory prefix for RAFT logs and snapshots, will have node-id appended to it")
	fs.StringVar(&c.BindAddr, "raft-bind", ":7000", "RAFT bind address")
	fs.BoolVar(&c.Bootstrap, "raft-bootstrap", false, "bootstrap a new RAFT cluster (first node only), this will override raft-join")
	fs.StringVar(&c.JoinAddr, "raft-join", ":7000", "RAFT address of node whose cluster we wish to join")
}

// Start RAFT if enabled
func (s *Server) InitRAFT() error {
	if !s.raftConfig.Enabled {
		log.Println("[raft]: disabled, running in single-node mode")
		return nil
	}

	// Validate required config
	if s.raftConfig.NodeID == "" {
		return fmt.Errorf("raft-node-id is required when RAFT is enabled")
	}

	if !s.raftConfig.Bootstrap && s.raftConfig.JoinAddr == "" {
		return fmt.Errorf("either raft-bootstrap or raft-join need to be set")
	}

	if s.raftConfig.Bootstrap {
		s.raftConfig.JoinAddr = ""
	}

	// Node ID suffix to data dir
	s.raftConfig.DataDir = filepath.Join(s.raftConfig.DataDir, s.raftConfig.NodeID)

	log.Printf("[raft]: initializing RAFT node=%s bind=%s datadir=%s", s.raftConfig.NodeID, s.raftConfig.BindAddr, s.raftConfig.DataDir)

	// Create data dir
	if err := os.MkdirAll(s.raftConfig.DataDir, 0755); err != nil {
		return err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.raftConfig.NodeID)
	config.SnapshotInterval = 2 * time.Minute
	config.SnapshotThreshold = 1024

	// Create RAFT client
	addr, err := net.ResolveTCPAddr("tcp", s.raftConfig.BindAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.raftConfig.BindAddr, addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return err
	}
	s.transport = transport

	// Create BoltDB log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.raftConfig.DataDir, "raft-log.db"))
	if err != nil {
		return err
	}
	s.logStore = logStore
	s.stableStore = logStore

	// Create file-based snapshot store
	snapStore, err := raft.NewFileSnapshotStore(s.raftConfig.DataDir, 3, os.Stdout)
	if err != nil {
		return err
	}
	s.snapStore = snapStore

	// Create RAFT instance
	s.fsm = &controlPlaneFSM{server: s}

	ra, err := raft.NewRaft(config, s.fsm, logStore, s.stableStore, snapStore, transport)
	if err != nil {
		return err
	}
	s.raft = ra

	if s.grpcListenAddr != "" {
		s.grpcAddrsMu.Lock()
		s.grpcAddrs[s.raftConfig.NodeID] = s.grpcListenAddr
		s.grpcAddrsMu.Unlock()
	}

	if s.raftConfig.Bootstrap {
		// Bootstrap cluster

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		raftFuture := ra.BootstrapCluster(configuration)
		if raftFuture.Error() != nil {
			return raftFuture.Error()
		}

		log.Printf("[raft]: bootstrapped new RAFT cluster")
	} else {
		// Join cluster

		err := s.joinCluster()
		if err != nil {
			return err
		}
	}

	log.Printf("[raft]: RAFT initialization complete")
	return nil
}

func (s *Server) joinCluster() error {
	log.Printf("[raft]: attempting to join cluster join-addr=%s", s.raftConfig.JoinAddr)

	conn, err := grpc.NewClient(s.raftConfig.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	req := &pb.JoinRAFTClusterRequest{
		NodeId:      s.raftConfig.NodeID,
		Address:     string(s.transport.LocalAddr()),
		GrpcAddress: s.grpcListenAddr,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.JoinRAFTCluster(ctx, req)
	if err != nil {
		return err
	}

	s.grpcAddrsMu.Lock()
	for nodeID, grpcAddr := range resp.GetClusterMembers() {
		s.grpcAddrs[nodeID] = grpcAddr
	}
	s.grpcAddrsMu.Unlock()

	log.Printf("[raft]: successfully joined RAFT cluster at %s", s.raftConfig.JoinAddr)
	return nil
}

func (s *Server) Shutdown() error {
	if s.raft != nil {
		log.Println("[raft]: shutting down RAFT")
		future := s.raft.Shutdown()
		if future.Error() != nil {
			return future.Error()
		}
	}

	if s.transport != nil {
		err := s.transport.Close()
		if err != nil {
			return err
		}
	}

	log.Println("[raft]: shutdown complete")
	return nil
}

func (s *Server) JoinRAFTCluster(ctx context.Context, req *pb.JoinRAFTClusterRequest) (*pb.JoinRAFTClusterResponse, error) {
	if s.raft == nil {
		return nil, status.Error(codes.FailedPrecondition, "RAFT not enabled")
	}

	// If not leader, forward to leader
	if !s.isRAFTLeader() {
		return forwardToLeader(s, status.Error(codes.FailedPrecondition, "not the leader"),
			func(client pb.ControlPlaneClient) (*pb.JoinRAFTClusterResponse, error) {
				return client.JoinRAFTCluster(ctx, req)
			})
	}

	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id required")
	}
	address := req.GetAddress()
	if address == "" {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	grpcAddress := req.GetGrpcAddress()
	if grpcAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "grpc_address required")
	}

	log.Printf("[raft]: adding RAFT server node=%s addr=%s grpc=%s", nodeID, address, grpcAddress)

	s.grpcAddrsMu.Lock()
	s.grpcAddrs[nodeID] = grpcAddress
	s.grpcAddrsMu.Unlock()

	future := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0)

	if future.Error() != nil {
		return nil, future.Error()
	}

	log.Printf("[raft]: successfully added RAFT server node=%s  addr=%s", nodeID, address)

	s.grpcAddrsMu.RLock()
	// Copy map to ensure no async shenanigans happen
	clusterMembers := make(map[string]string)
	for id, addr := range s.grpcAddrs {
		clusterMembers[id] = addr
	}
	s.grpcAddrsMu.RUnlock()

	return &pb.JoinRAFTClusterResponse{
		ClusterMembers: clusterMembers,
	}, nil
}

func (s *Server) LeaveRAFTCluster(ctx context.Context, req *pb.LeaveRAFTClusterRequest) (*emptypb.Empty, error) {
	if s.raft == nil {
		return nil, status.Error(codes.FailedPrecondition, "RAFT not enabled")
	}

	if !s.isRAFTLeader() {
		return forwardToLeader(s, status.Error(codes.FailedPrecondition, "not the leader"),
			func(client pb.ControlPlaneClient) (*emptypb.Empty, error) {
				return client.LeaveRAFTCluster(ctx, req)
			})
	}

	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id required")
	}

	log.Printf("[raft]: removing RAFT server node=%s", nodeID)

	future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)

	if future.Error() != nil {
		return nil, future.Error()
	}

	log.Printf("[raft]: successfully removed RAFT server node=%s", nodeID)
	return &emptypb.Empty{}, nil
}

func (s *Server) GetRAFTStatus(ctx context.Context, _ *emptypb.Empty) (*pb.RAFTStatusResponse, error) {
	if s.raft == nil {
		return &pb.RAFTStatusResponse{State: "Disabled"}, nil
	}

	configFuture := s.raft.GetConfiguration()
	if configFuture.Error() != nil {
		return nil, configFuture.Error()
	}

	peers := []string{}
	for _, server := range configFuture.Configuration().Servers {
		peers = append(peers, string(server.ID))
	}

	// Build response with all status information
	return &pb.RAFTStatusResponse{
		State:        s.raft.State().String(),
		Leader:       string(s.raft.Leader()),
		Peers:        peers,
		LastLogIndex: s.raft.LastIndex(),
		CommitIndex:  s.raft.CommitIndex(),
		AppliedIndex: s.raft.AppliedIndex(),
	}, nil
}
