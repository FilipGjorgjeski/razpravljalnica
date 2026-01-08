package server

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newTestClient(t *testing.T) (pb.MessageBoardClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	st := storage.New()
	h := NewHub()
	svc := NewServices(st, h, "node-test", "127.0.0.1:0")

	s := grpc.NewServer()
	svc.Register(s)

	go func() {
		_ = s.Serve(lis)
	}()

	addr := lis.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.Stop()
		_ = lis.Close()
		t.Fatalf("NewClient: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		s.Stop()
		_ = lis.Close()
	}

	return pb.NewMessageBoardClient(conn), cleanup
}

func TestService_CreateAndPostFlow(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	user, err := c.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	topic, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg, err := c.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.Id, UserId: user.Id, Text: "hello"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if msg.TopicId != topic.Id {
		t.Fatalf("unexpected TopicId: got=%d want=%d", msg.TopicId, topic.Id)
	}
	if msg.Text != "hello" {
		t.Fatalf("unexpected text: got=%q", msg.Text)
	}
}

func TestService_SubscribeTopic_BacklogThenLive(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	user, err := c.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	topic, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	subNode, err := c.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic.Id}})
	if err != nil {
		t.Fatalf("GetSubscriptionNode: %v", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	stream, err := c.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic.Id}, UserId: user.Id, SubscribeToken: subNode.SubscribeToken})
	if err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}
	defer subCancel()

	// Create backlog event.
	_, err = c.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.Id, UserId: user.Id, Text: "before"})
	if err != nil {
		t.Fatalf("PostMessage(before): %v", err)
	}

	// First message should come from backlog.
	ev1, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv backlog: %v", err)
	}
	if ev1.GetMessage() == nil || ev1.GetMessage().GetText() != "before" {
		t.Fatalf("unexpected backlog event: %+v", ev1)
	}

	// Now produce a live event.
	_, err = c.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.Id, UserId: user.Id, Text: "after"})
	if err != nil {
		t.Fatalf("PostMessage(after): %v", err)
	}

	// Should receive the live event.
	ev2, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv live: %v", err)
	}
	if ev2.GetMessage() == nil || ev2.GetMessage().GetText() != "after" {
		t.Fatalf("unexpected live event: %+v", ev2)
	}
}

func TestService_LikeMessage_DuplicateDoesNotBroadcast(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ana, err := c.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	borko, err := c.CreateUser(ctx, &pb.CreateUserRequest{Name: "borko"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	topic, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg, err := c.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topic.Id, UserId: ana.Id, Text: "hi"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	subNode, err := c.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: borko.Id, TopicId: []int64{topic.Id}})
	if err != nil {
		t.Fatalf("GetSubscriptionNode: %v", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	stream, err := c.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic.Id}, UserId: borko.Id, SubscribeToken: subNode.SubscribeToken})
	if err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}
	defer subCancel()

	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv backlog: %v", err)
	}

	// First like should broadcast.
	_, err = c.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topic.Id, MessageId: msg.Id, UserId: borko.Id})
	if err != nil {
		t.Fatalf("LikeMessage(1): %v", err)
	}
	ev1, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv like event: %v", err)
	}
	if ev1.GetOp() != pb.OpType_OP_LIKE {
		t.Fatalf("unexpected op: %v", ev1.GetOp())
	}

	// Second like (duplicate) should NOT broadcast; ensure we don't block forever.
	_, err = c.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topic.Id, MessageId: msg.Id, UserId: borko.Id})
	if err != nil {
		t.Fatalf("LikeMessage(2): %v", err)
	}

	readDone := make(chan struct{})
	go func() {
		_, _ = stream.Recv()
		close(readDone)
	}()

	select {
	case <-readDone:
		t.Fatalf("unexpected event received for duplicate like")
	case <-time.After(200 * time.Millisecond):
		// ok
	}
	// Stop the stream so the goroutine unblocks.
	subCancel()
	<-readDone
}
