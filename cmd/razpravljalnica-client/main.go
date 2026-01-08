package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/client"
)

func main() {
	var cpAddr string
	var watchFor time.Duration
	flag.StringVar(&cpAddr, "control-plane", "127.0.0.1:50051", "control-plane gRPC address")
	flag.DurationVar(&watchFor, "watch-for", 0, "if >0, keep watching cluster state updates for this long")
	flag.Parse()

	c, err := client.New(cpAddr)
	if err != nil {
		log.Fatalf("connect control-plane: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := client.WithTimeout(context.Background(), 5*time.Second)
	st, err := c.RefreshClusterState(ctx)
	cancel()
	if err != nil {
		log.Fatalf("GetClusterState: %v", err)
	}
	fmt.Printf("cluster v=%d head=%v tail=%v\n", st.ConfigVersion, st.Head, st.Tail)

	watchCtx, watchCancel := context.WithCancel(context.Background())
	if watchFor > 0 {
		go func() {
			if err := c.WatchClusterState(watchCtx); err != nil {
				fmt.Fprintf(os.Stderr, "watch stopped: %v\n", err)
			}
		}()
	}

	// Scripted scenario.
	call := func(d time.Duration) (context.Context, context.CancelFunc) {
		return client.WithTimeout(context.Background(), d)
	}

	ctx, cancel = call(4 * time.Second)
	u, err := c.CreateUser(ctx, "alice", "req-u1")
	cancel()
	if err != nil {
		log.Fatalf("CreateUser: %v", err)
	}
	fmt.Printf("user: id=%d\n", u.GetId())

	ctx, cancel = call(4 * time.Second)
	t, err := c.CreateTopic(ctx, "general", "req-t1")
	cancel()
	if err != nil {
		log.Fatalf("CreateTopic: %v", err)
	}
	fmt.Printf("topic: id=%d\n", t.GetId())

	ctx, cancel = call(4 * time.Second)
	m1, err := c.PostMessage(ctx, t.GetId(), u.GetId(), "hello", "req-m1")
	cancel()
	if err != nil {
		log.Fatalf("PostMessage: %v", err)
	}
	fmt.Printf("message: id=%d likes=%d\n", m1.GetId(), m1.GetLikes())

	// Retry (idempotent).
	ctx, cancel = call(4 * time.Second)
	m2, err := c.PostMessage(ctx, t.GetId(), u.GetId(), "hello", "req-m1")
	cancel()
	if err != nil {
		log.Fatalf("PostMessage retry: %v", err)
	}
	fmt.Printf("message retry: id=%d likes=%d\n", m2.GetId(), m2.GetLikes())

	ctx, cancel = call(4 * time.Second)
	msgs, err := c.GetMessages(ctx, t.GetId(), 0, 10)
	cancel()
	if err != nil {
		log.Fatalf("GetMessages: %v", err)
	}
	fmt.Printf("tail messages: n=%d\n", len(msgs.GetMessages()))

	ctx, cancel = call(4 * time.Second)
	liked1, err := c.LikeMessage(ctx, t.GetId(), m1.GetId(), u.GetId(), "req-like-1")
	cancel()
	if err != nil {
		log.Fatalf("LikeMessage 1: %v", err)
	}
	fmt.Printf("like1: likes=%d\n", liked1.GetLikes())

	ctx, cancel = call(4 * time.Second)
	liked2, err := c.LikeMessage(ctx, t.GetId(), m1.GetId(), u.GetId(), "req-like-2")
	cancel()
	if err != nil {
		log.Fatalf("LikeMessage 2: %v", err)
	}
	fmt.Printf("like2: likes=%d\n", liked2.GetLikes())

	if watchFor > 0 {
		t0 := time.Now()
		for time.Since(t0) < watchFor {
			cur := c.State()
			fmt.Printf("watch: v=%d head=%v tail=%v\n", cur.ConfigVersion, cur.Head, cur.Tail)
			time.Sleep(1 * time.Second)
		}
		watchCancel()
		// Give watch loop a moment to exit.
		time.Sleep(50 * time.Millisecond)
	}
}
