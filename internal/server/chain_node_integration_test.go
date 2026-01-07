package server

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func startGRPC(t *testing.T, register func(*grpc.Server), lis net.Listener) *grpc.Server {
	t.Helper()
	gs := grpc.NewServer()
	register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return gs
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestChainNode_TwoNodeCommitAndIdempotentRetry(t *testing.T) {
	// Start head.
	headLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	headAddr := headLis.Addr().String()
	defer headLis.Close()

	head := NewChainNode(storage.New(), NewHub(), "n1", headAddr, headAddr, "")
	startGRPC(t, head.Register, headLis)

	// Start tail.
	tailLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tailAddr := tailLis.Addr().String()
	defer tailLis.Close()

	tail := NewChainNode(storage.New(), NewHub(), "n2", tailAddr, tailAddr, "")
	startGRPC(t, tail.Register, tailLis)

	// Set roles and neighbors manually (no control-plane goroutine in this test).
	headInfo := &pb.NodeInfo{NodeId: "n1", Address: headAddr}
	tailInfo := &pb.NodeInfo{NodeId: "n2", Address: tailAddr}

	head.mu.Lock()
	head.role = pb.ChainRole_ROLE_HEAD
	head.head = headInfo
	head.tail = tailInfo
	head.pred = nil
	head.succ = tailInfo
	head.mu.Unlock()

	tail.mu.Lock()
	tail.role = pb.ChainRole_ROLE_TAIL
	tail.head = headInfo
	tail.tail = tailInfo
	tail.pred = headInfo
	tail.succ = nil
	tail.mu.Unlock()

	head.Start()
	tail.Start()

	headConn := dial(t, headAddr)
	mbHead := pb.NewMessageBoardClient(headConn)
	replHead := pb.NewReplicationClient(headConn)

	tailConn := dial(t, tailAddr)
	mbTail := pb.NewMessageBoardClient(tailConn)
	replTail := pb.NewReplicationClient(tailConn)

	callCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 2*time.Second)
	}

	// Write flow: CreateUser -> CreateTopic -> PostMessage.
	ctx, cancel := callCtx()
	user, err := mbHead.CreateUser(ctx, &pb.CreateUserRequest{Name: "alice", RequestId: "req-u1"})
	cancel()
	require.NoError(t, err)
	require.NotZero(t, user.GetId())

	ctx, cancel = callCtx()
	topic, err := mbHead.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "general", RequestId: "req-t1"})
	cancel()
	require.NoError(t, err)
	require.NotZero(t, topic.GetId())

	ctx, cancel = callCtx()
	msg1, err := mbHead.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.GetId(), UserId: user.GetId(), Text: "hello", RequestId: "req-m1"})
	cancel()
	require.NoError(t, err)
	require.NotZero(t, msg1.GetId())

	// Verify commit reached both nodes.
	require.Eventually(t, func() bool {
		ctx, cancel := callCtx()
		defer cancel()
		s1, err := replHead.GetStatus(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		s2, err := replTail.GetStatus(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		return s1.GetLastCommitted() == s2.GetLastCommitted() &&
			s1.GetLastApplied() == s2.GetLastApplied() &&
			s1.GetLastCommitted() >= 3
	}, 3*time.Second, 50*time.Millisecond)

	// Idempotent retry: same request_id must return exact same message and not advance log.
	ctx, cancel = callCtx()
	before, err := replHead.GetStatus(ctx, &emptypb.Empty{})
	cancel()
	require.NoError(t, err)

	ctx, cancel = callCtx()
	msg2, err := mbHead.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.GetId(), UserId: user.GetId(), Text: "hello", RequestId: "req-m1"})
	cancel()
	require.NoError(t, err)
	require.Equal(t, msg1.GetId(), msg2.GetId())

	ctx, cancel = callCtx()
	after, err := replHead.GetStatus(ctx, &emptypb.Empty{})
	cancel()
	require.NoError(t, err)
	require.Equal(t, before.GetLastApplied(), after.GetLastApplied())
	require.Equal(t, before.GetLastCommitted(), after.GetLastCommitted())

	// Read from tail should show the message.
	ctx, cancel = callCtx()
	msgs, err := mbTail.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.GetId(), FromMessageId: 0, Limit: 10})
	cancel()
	require.NoError(t, err)
	require.Len(t, msgs.GetMessages(), 1)
	require.Equal(t, msg1.GetId(), msgs.GetMessages()[0].GetId())
	require.Equal(t, int32(0), msgs.GetMessages()[0].GetLikes())

	// Like twice with different request IDs by the same user: likes must stay 1.
	ctx, cancel = callCtx()
	liked1, err := mbHead.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topic.GetId(), MessageId: msg1.GetId(), UserId: user.GetId(), RequestId: "req-like-1"})
	cancel()
	require.NoError(t, err)
	require.Equal(t, int32(1), liked1.GetLikes())

	ctx, cancel = callCtx()
	liked2, err := mbHead.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topic.GetId(), MessageId: msg1.GetId(), UserId: user.GetId(), RequestId: "req-like-2"})
	cancel()
	require.NoError(t, err)
	require.Equal(t, int32(1), liked2.GetLikes())

	ctx, cancel = callCtx()
	msgs2, err := mbTail.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.GetId(), FromMessageId: 0, Limit: 10})
	cancel()
	require.NoError(t, err)
	require.Len(t, msgs2.GetMessages(), 1)
	require.Equal(t, int32(1), msgs2.GetMessages()[0].GetLikes())
}

func TestChainNode_ReplicateOutOfOrderReturnsFailedPrecondition(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	defer lis.Close()

	n := NewChainNode(storage.New(), NewHub(), "n1", addr, addr, "")
	n.mu.Lock()
	n.role = pb.ChainRole_ROLE_TAIL
	n.succ = nil
	n.mu.Unlock()
	n.Start()
	startGRPC(t, n.Register, lis)

	conn := dial(t, addr)
	c := pb.NewReplicationClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First replicated entry is out-of-order (Seq=2 while applied starts at 0).
	_, err = c.Replicate(ctx, &pb.ReplicateRequest{Entry: &pb.LogEntry{
		SequenceNumber: 2,
		At:             timestamppb.Now(),
		Op:             &pb.LogEntry_CreateUser{CreateUser: &pb.CreateUserOp{UserId: 1, Name: "alice"}},
	}})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestSubscriptionRoutingAndTokenEnforcement(t *testing.T) {
	// Start head.
	headLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	headAddr := headLis.Addr().String()
	defer headLis.Close()

	head := NewChainNode(storage.New(), NewHub(), "n1", headAddr, headAddr, "")
	startGRPC(t, head.Register, headLis)

	// Start tail.
	tailLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tailAddr := tailLis.Addr().String()
	defer tailLis.Close()

	tail := NewChainNode(storage.New(), NewHub(), "n2", tailAddr, tailAddr, "")
	startGRPC(t, tail.Register, tailLis)

	headInfo := &pb.NodeInfo{NodeId: "n1", Address: headAddr}
	tailInfo := &pb.NodeInfo{NodeId: "n2", Address: tailAddr}

	head.mu.Lock()
	head.role = pb.ChainRole_ROLE_HEAD
	head.head = headInfo
	head.tail = tailInfo
	head.pred = nil
	head.succ = tailInfo
	head.chain = []*pb.NodeInfo{headInfo, tailInfo}
	head.mu.Unlock()

	tail.mu.Lock()
	tail.role = pb.ChainRole_ROLE_TAIL
	tail.head = headInfo
	tail.tail = tailInfo
	tail.pred = headInfo
	tail.succ = nil
	tail.chain = []*pb.NodeInfo{headInfo, tailInfo}
	tail.mu.Unlock()

	head.Start()
	tail.Start()

	headConn := dial(t, headAddr)
	mbHead := pb.NewMessageBoardClient(headConn)
	replHead := pb.NewReplicationClient(headConn)

	tailConn := dial(t, tailAddr)
	mbTail := pb.NewMessageBoardClient(tailConn)
	replTail := pb.NewReplicationClient(tailConn)

	callCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 2*time.Second)
	}

	ctx, cancel := callCtx()
	user, err := mbHead.CreateUser(ctx, &pb.CreateUserRequest{Name: "alice", RequestId: "req-sub-u"})
	cancel()
	require.NoError(t, err)
	require.NotZero(t, user.GetId())

	ctx, cancel = callCtx()
	topic, err := mbHead.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "general", RequestId: "req-sub-t"})
	cancel()
	require.NoError(t, err)
	require.NotZero(t, topic.GetId())

	// Ensure chain is in sync before subscription registration.
	require.Eventually(t, func() bool {
		ctx, cancel := callCtx()
		defer cancel()
		hs, err := replHead.GetStatus(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		ts, err := replTail.GetStatus(ctx, &emptypb.Empty{})
		if err != nil {
			return false
		}
		return hs.GetLastCommitted() == ts.GetLastCommitted()
	}, 2*time.Second, 50*time.Millisecond)

	ctx, cancel = callCtx()
	subResp, err := mbHead.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user.GetId(), TopicId: []int64{topic.GetId()}})
	cancel()
	require.NoError(t, err)
	require.NotEmpty(t, subResp.GetSubscribeToken())
	require.Equal(t, tailInfo.GetNodeId(), subResp.GetNode().GetNodeId())
	require.Equal(t, tailInfo.GetAddress(), subResp.GetNode().GetAddress())

	// Using the token on the wrong node should fail.
	ctx, cancel = callCtx()
	wrongStream, err := mbHead.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{TopicId: []int64{topic.GetId()}, UserId: user.GetId(), SubscribeToken: subResp.GetSubscribeToken()})
	require.NoError(t, err)
	_, err = wrongStream.Recv()
	cancel()
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, codes.Unauthenticated, st.Code())

	// Subscribe on the assigned node and ensure events flow.
	ctx, cancel = callCtx()
	stream, err := mbTail.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{TopicId: []int64{topic.GetId()}, UserId: user.GetId(), SubscribeToken: subResp.GetSubscribeToken()})
	require.NoError(t, err)

	// Post a message to generate an event.
	ctxPost, cancelPost := callCtx()
	_, err = mbHead.PostMessage(ctxPost, &pb.PostMessageRequest{TopicId: topic.GetId(), UserId: user.GetId(), Text: "hi", RequestId: "req-sub-msg"})
	cancelPost()
	require.NoError(t, err)

	evCh := make(chan *pb.MessageEvent, 1)
	go func() {
		defer close(evCh)
		ev, err := stream.Recv()
		if err != nil {
			return
		}
		evCh <- ev
	}()

	require.Eventually(t, func() bool {
		select {
		case ev := <-evCh:
			return ev != nil && ev.GetMessage().GetText() == "hi"
		default:
			return false
		}
	}, 3*time.Second, 50*time.Millisecond)
	cancel()
}
