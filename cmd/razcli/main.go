package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/client"
	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type globalOpts struct {
	controlPlane string
	timeout      time.Duration
}

func main() {
	opts, rest := parseGlobal(os.Args[1:])
	if len(rest) == 0 {
		usage()
		os.Exit(1)
	}

	var err error
	switch rest[0] {
	case "cluster":
		err = cmdCluster(opts, rest[1:])
	case "node":
		err = cmdNode(opts, rest[1:])
	case "user":
		err = cmdUser(opts, rest[1:])
	case "topic":
		err = cmdTopic(opts, rest[1:])
	case "message":
		err = cmdMessage(opts, rest[1:])
	case "subscribe":
		err = cmdSubscribe(opts, rest[1:])
	default:
		usage()
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func parseGlobal(args []string) (globalOpts, []string) {
	fs := flag.NewFlagSet("razcli", flag.ExitOnError)
	var opts globalOpts
	fs.StringVar(&opts.controlPlane, "control-plane", "127.0.0.1:50050", "control-plane gRPC address")
	fs.DurationVar(&opts.timeout, "timeout", 5*time.Second, "per-RPC timeout")
	_ = fs.Parse(args)
	return opts, fs.Args()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: razcli [global flags] <command> [subcommand] [flags]\n")
	fmt.Fprintf(os.Stderr, "Commands: cluster, node, user, topic, message, subscribe\n")
}

func dialControlPlane(addr string) (pb.ControlPlaneClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewControlPlaneClient(conn), conn, nil
}

func cmdCluster(opts globalOpts, args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	watchFor := fs.Duration("watch-for", 0, "watch updates for this duration (0=single fetch)")
	_ = fs.Parse(args)

	cli, conn, err := dialControlPlane(opts.controlPlane)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	st, err := cli.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	printClusterState(st)

	if *watchFor <= 0 {
		return nil
	}

	wctx, wcancel := context.WithTimeout(context.Background(), *watchFor)
	defer wcancel()
	stream, err := cli.WatchClusterState(wctx, &pb.WatchClusterStateRequest{})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		printClusterState(resp)
	}
}

func cmdNode(opts globalOpts, args []string) error {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: razcli node <add|activate> [flags]\n")
		os.Exit(1)
	}
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("node add", flag.ExitOnError)
		id := fs.String("id", "", "node id")
		addr := fs.String("addr", "", "node address")
		_ = fs.Parse(args[1:])
		cli, conn, err := dialControlPlane(opts.controlPlane)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		resp, err := cli.AddNode(ctx, &pb.AddNodeRequest{NodeId: *id, Address: *addr})
		if err != nil {
			return err
		}
		fmt.Printf("pending node added; current tail: %v\n", resp.GetCurrentTail())
		return nil
	case "activate":
		fs := flag.NewFlagSet("node activate", flag.ExitOnError)
		id := fs.String("id", "", "node id")
		_ = fs.Parse(args[1:])
		cli, conn, err := dialControlPlane(opts.controlPlane)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		resp, err := cli.ActivateNode(ctx, &pb.ActivateNodeRequest{NodeId: *id})
		if err != nil {
			return err
		}
		printClusterState(resp)
		return nil
	default:
		fmt.Fprintf(os.Stderr, "Usage: razcli node <add|activate> [flags]\n")
		os.Exit(1)
	}
	return nil
}

func cmdUser(opts globalOpts, args []string) error {
	fs := flag.NewFlagSet("user create", flag.ExitOnError)
	name := fs.String("name", "", "user name")
	_ = fs.Parse(args)

	c, err := client.New(opts.controlPlane)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	u, err := c.CreateUser(ctx, *name, "")
	if err != nil {
		return err
	}
	fmt.Printf("user created id=%d name=%s\n", u.GetId(), u.GetName())
	return nil
}

func cmdTopic(opts globalOpts, args []string) error {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: razcli topic <create|list> [flags]\n")
		os.Exit(1)
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("topic create", flag.ExitOnError)
		name := fs.String("name", "", "topic name")
		_ = fs.Parse(args[1:])

		c, err := client.New(opts.controlPlane)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		t, err := c.CreateTopic(ctx, *name, "")
		if err != nil {
			return err
		}
		fmt.Printf("topic created id=%d name=%s\n", t.GetId(), t.GetName())
		return nil
	case "list":
		fs := flag.NewFlagSet("topic list", flag.ExitOnError)
		_ = fs.Parse(args[1:])

		c, err := client.New(opts.controlPlane)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		resp, err := c.ListTopics(ctx)
		if err != nil {
			return err
		}
		for _, t := range resp.GetTopics() {
			fmt.Printf("id=%d name=%s\n", t.GetId(), t.GetName())
		}
		return nil
	default:
		fmt.Fprintf(os.Stderr, "Usage: razcli topic <create|list> [flags]\n")
		os.Exit(1)
	}
	return nil
}

func cmdMessage(opts globalOpts, args []string) error {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: razcli message <post|update|delete|like|list> [flags]\n")
		os.Exit(1)
	}

	c, err := client.New(opts.controlPlane)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	switch args[0] {
	case "post":
		fs := flag.NewFlagSet("message post", flag.ExitOnError)
		topicID := fs.Int64("topic", 0, "topic id")
		userID := fs.Int64("user", 0, "user id")
		text := fs.String("text", "", "message text")
		_ = fs.Parse(args[1:])
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		msg, err := c.PostMessage(ctx, *topicID, *userID, *text, "")
		if err != nil {
			return err
		}
		fmt.Printf("message id=%d likes=%d\n", msg.GetId(), msg.GetLikes())
		return nil
	case "update":
		fs := flag.NewFlagSet("message update", flag.ExitOnError)
		topicID := fs.Int64("topic", 0, "topic id")
		userID := fs.Int64("user", 0, "user id")
		msgID := fs.Int64("id", 0, "message id")
		text := fs.String("text", "", "new text")
		_ = fs.Parse(args[1:])
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		msg, err := c.UpdateMessage(ctx, *topicID, *userID, *msgID, *text, "")
		if err != nil {
			return err
		}
		fmt.Printf("message updated id=%d text=%s\n", msg.GetId(), msg.GetText())
		return nil
	case "delete":
		fs := flag.NewFlagSet("message delete", flag.ExitOnError)
		topicID := fs.Int64("topic", 0, "topic id")
		userID := fs.Int64("user", 0, "user id")
		msgID := fs.Int64("id", 0, "message id")
		_ = fs.Parse(args[1:])
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		if err := c.DeleteMessage(ctx, *topicID, *userID, *msgID, ""); err != nil {
			return err
		}
		fmt.Printf("message deleted id=%d\n", *msgID)
		return nil
	case "like":
		fs := flag.NewFlagSet("message like", flag.ExitOnError)
		topicID := fs.Int64("topic", 0, "topic id")
		userID := fs.Int64("user", 0, "user id")
		msgID := fs.Int64("id", 0, "message id")
		_ = fs.Parse(args[1:])
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		msg, err := c.LikeMessage(ctx, *topicID, *msgID, *userID, "")
		if err != nil {
			return err
		}
		fmt.Printf("message liked id=%d likes=%d\n", msg.GetId(), msg.GetLikes())
		return nil
	case "list":
		fs := flag.NewFlagSet("message list", flag.ExitOnError)
		topicID := fs.Int64("topic", 0, "topic id")
		fromID := fs.Int64("from", 0, "from message id")
		limit := fs.Int("limit", 50, "limit")
		_ = fs.Parse(args[1:])
		ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
		defer cancel()
		resp, err := c.GetMessages(ctx, *topicID, *fromID, int32(*limit))
		if err != nil {
			return err
		}
		for _, m := range resp.GetMessages() {
			fmt.Printf("id=%d user=%d text=%s likes=%d\n", m.GetId(), m.GetUserId(), m.GetText(), m.GetLikes())
		}
		return nil
	default:
		fmt.Fprintf(os.Stderr, "Usage: razcli message <post|update|delete|like|list> [flags]\n")
		os.Exit(1)
	}
	return nil
}

func cmdSubscribe(opts globalOpts, args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ExitOnError)
	topicsRaw := fs.String("topics", "", "comma-separated topic ids")
	userID := fs.Int64("user", 0, "user id")
	fromID := fs.Int64("from", 0, "from message id")
	_ = fs.Parse(args)

	topics, err := parseInt64List(*topicsRaw)
	if err != nil {
		return err
	}

	c, err := client.New(opts.controlPlane)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(ev *pb.MessageEvent) error {
		fmt.Printf("seq=%d op=%s topic=%d msg=%d user=%d text=%s likes=%d\n", ev.GetSequenceNumber(), ev.GetOp().String(), ev.GetMessage().GetTopicId(), ev.GetMessage().GetId(), ev.GetMessage().GetUserId(), ev.GetMessage().GetText(), ev.GetMessage().GetLikes())
		return nil
	}

	return c.Subscribe(ctx, topics, *userID, *fromID, handler)
}

func printClusterState(st *pb.GetClusterStateResponse) {
	fmt.Printf("config=%d head=%v tail=%v chain=%v\n", st.GetConfigVersion(), st.GetHead(), st.GetTail(), st.GetChain())
}

func parseInt64List(s string) ([]int64, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("topics required")
	}
	parts := strings.Split(s, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("topics required")
	}
	return out, nil
}
