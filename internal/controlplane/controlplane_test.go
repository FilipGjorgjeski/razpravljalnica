package controlplane

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestControlPlane_RoleAndNeighbors(t *testing.T) {
	s := New(Config{ChainOrder: []string{"n1", "n2", "n3"}, HeartbeatTimeout: 5 * time.Second})
	ctx := context.Background()

	_, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	_, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222"})
	require.NoError(t, err)
	resp3, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n3", Address: "127.0.0.1:3333"})
	require.NoError(t, err)

	resp1, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	require.Equal(t, pb.ChainRole_ROLE_HEAD, resp1.GetRole())
	require.NotNil(t, resp1.GetSuccessor())
	require.Equal(t, "n2", resp1.GetSuccessor().GetNodeId())
	require.Nil(t, resp1.GetPredecessor())

	resp2, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222"})
	require.NoError(t, err)
	require.Equal(t, pb.ChainRole_ROLE_MIDDLE, resp2.GetRole())
	require.Equal(t, "n1", resp2.GetPredecessor().GetNodeId())
	require.Equal(t, "n3", resp2.GetSuccessor().GetNodeId())

	require.Equal(t, pb.ChainRole_ROLE_TAIL, resp3.GetRole())
	require.Equal(t, "n2", resp3.GetPredecessor().GetNodeId())
	require.Nil(t, resp3.GetSuccessor())
}

func TestControlPlane_ConfigVersionBumpsOnChainChange(t *testing.T) {
	s := New(Config{ChainOrder: []string{"n1", "n2"}, HeartbeatTimeout: 50 * time.Millisecond})
	ctx := context.Background()

	resp, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	ver1 := resp.GetConfigVersion()

	resp, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222"})
	require.NoError(t, err)
	ver2 := resp.GetConfigVersion()
	require.GreaterOrEqual(t, ver2, ver1)

	state, err := s.GetClusterState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	verBefore := state.GetConfigVersion()
	require.Len(t, state.GetChain(), 2)

	chain, _, _, verAfter, _ := s.computeChain(time.Now().UTC().Add(10 * time.Second))
	require.Len(t, chain, 0)
	require.Greater(t, verAfter, verBefore)

	resp, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	require.Greater(t, resp.GetConfigVersion(), verAfter)
}

func TestControlPlane_WatchClusterStatePushesUpdates(t *testing.T) {
	// Start a real gRPC server to test streaming semantics.
	srv := New(Config{ChainOrder: []string{"n1", "n2"}, HeartbeatTimeout: 100 * time.Millisecond})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := pb.NewControlPlaneClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := c.WatchClusterState(ctx, &pb.WatchClusterStateRequest{})
	require.NoError(t, err)

	// Initial state.
	init, err := stream.Recv()
	require.NoError(t, err)
	initVer := init.GetConfigVersion()

	// Trigger a chain change by heartbeating nodes.
	_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	_, err = c.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222"})
	require.NoError(t, err)

	var upd *pb.GetClusterStateResponse
	for {
		m, err := stream.Recv()
		require.NoError(t, err)
		if m.GetConfigVersion() > initVer && len(m.GetChain()) == 2 {
			upd = m
			break
		}
	}
	require.NotNil(t, upd)
	require.NotNil(t, upd.GetHead())
	require.NotNil(t, upd.GetTail())
	require.Len(t, upd.GetChain(), 2)
}

func TestControlPlane_AddAndActivateNode(t *testing.T) {
	s := New(Config{ChainOrder: []string{"n1", "n2"}, HeartbeatTimeout: 5 * time.Second})
	ctx := context.Background()

	_, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111"})
	require.NoError(t, err)
	_, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222"})
	require.NoError(t, err)

	resp, err := s.AddNode(ctx, &pb.AddNodeRequest{NodeId: "n3", Address: "127.0.0.1:3333"})
	require.NoError(t, err)
	require.NotNil(t, resp.GetCurrentTail())
	require.Equal(t, "n2", resp.GetCurrentTail().GetNodeId())

	state, err := s.GetClusterState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, state.GetChain(), 2)

	// Activate after node is presumed synced.
	act, err := s.ActivateNode(ctx, &pb.ActivateNodeRequest{NodeId: "n3"})
	require.NoError(t, err)
	require.Equal(t, "n1", act.GetHead().GetNodeId())
	require.Equal(t, "n3", act.GetTail().GetNodeId())
	require.Len(t, act.GetChain(), 3)

	// Idempotent activate should succeed and keep chain length.
	act2, err := s.ActivateNode(ctx, &pb.ActivateNodeRequest{NodeId: "n3"})
	require.NoError(t, err)
	require.Len(t, act2.GetChain(), 3)
}

func TestControlPlane_AutoPromotePendingNodeOnCatchup(t *testing.T) {
	s := New(Config{ChainOrder: []string{"n1", "n2"}, HeartbeatTimeout: 5 * time.Second})
	ctx := context.Background()

	// Seed chain with head and tail statuses.
	_, err := s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n1", Address: "127.0.0.1:1111", Status: &pb.NodeStatus{LastApplied: 5, LastCommitted: 5}})
	require.NoError(t, err)
	_, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n2", Address: "127.0.0.1:2222", Status: &pb.NodeStatus{LastApplied: 5, LastCommitted: 5}})
	require.NoError(t, err)

	// Add a new node; it becomes pending.
	_, err = s.AddNode(ctx, &pb.AddNodeRequest{NodeId: "n3", Address: "127.0.0.1:3333"})
	require.NoError(t, err)

	state, err := s.GetClusterState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, state.GetChain(), 2)
	require.Equal(t, "n2", state.GetTail().GetNodeId())

	// Once n3 reports it has caught up to the tail's committed seq, it should auto-promote.
	_, err = s.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "n3", Address: "127.0.0.1:3333", Status: &pb.NodeStatus{LastApplied: 5, LastCommitted: 5}})
	require.NoError(t, err)

	stateAfter, err := s.GetClusterState(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, stateAfter.GetChain(), 3)
	require.Equal(t, "n3", stateAfter.GetTail().GetNodeId())
}
