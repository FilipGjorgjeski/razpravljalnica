package client

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"sync"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClusterState struct {
	Head          *pb.NodeInfo
	Tail          *pb.NodeInfo
	Chain         []*pb.NodeInfo
	ConfigVersion int64
}

type Client struct {
	cpAddr string
	cpConn *grpc.ClientConn
	cp     pb.ControlPlaneClient

	mu    sync.RWMutex
	state ClusterState
}

func New(controlPlaneAddr string) (*Client, error) {
	conn, err := grpc.NewClient(controlPlaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		cpAddr: controlPlaneAddr,
		cpConn: conn,
		cp:     pb.NewControlPlaneClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c.cpConn != nil {
		return c.cpConn.Close()
	}
	return nil
}

func (c *Client) RefreshClusterState(ctx context.Context) (ClusterState, error) {
	resp, err := c.cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return ClusterState{}, err
	}

	st := ClusterState{
		Head:          resp.GetHead(),
		Tail:          resp.GetTail(),
		Chain:         resp.GetChain(),
		ConfigVersion: resp.GetConfigVersion(),
	}

	c.mu.Lock()
	c.state = st
	c.mu.Unlock()

	return st, nil
}

// WatchClusterState runs until ctx is canceled or the stream ends.
// It updates the in-memory cached ClusterState whenever a new config version arrives.
func (c *Client) WatchClusterState(ctx context.Context) error {
	c.mu.RLock()
	last := c.state.ConfigVersion
	c.mu.RUnlock()

	stream, err := c.cp.WatchClusterState(ctx, &pb.WatchClusterStateRequest{LastSeenConfigVersion: last})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		st := ClusterState{
			Head:          resp.GetHead(),
			Tail:          resp.GetTail(),
			Chain:         resp.GetChain(),
			ConfigVersion: resp.GetConfigVersion(),
		}

		c.mu.Lock()
		if st.ConfigVersion >= c.state.ConfigVersion {
			c.state = st
		}
		c.mu.Unlock()
	}
}

func (c *Client) State() ClusterState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func RequestID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func WithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, d)
}

func (c *Client) dialMessageBoard(addr string) (*grpc.ClientConn, pb.MessageBoardClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewMessageBoardClient(conn), nil
}

func (c *Client) headAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state.Head != nil {
		return c.state.Head.Address
	}
	return ""
}

func (c *Client) tailAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state.Tail != nil {
		return c.state.Tail.Address
	}
	return ""
}

func parseHeadRedirect(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	msg := st.Message()
	const prefix = "writes go to head: "
	idx := strings.Index(msg, prefix)
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(msg[idx+len(prefix):])
}

func (c *Client) CreateUser(ctx context.Context, name, requestID string) (*pb.User, error) {
	if requestID == "" {
		requestID = RequestID()
	}

	for attempt := 0; attempt < 2; attempt++ {
		addr := c.headAddr()
		if addr == "" {
			if _, err := c.RefreshClusterState(ctx); err != nil {
				return nil, err
			}
			addr = c.headAddr()
		}

		conn, mb, err := c.dialMessageBoard(addr)
		if err != nil {
			_, _ = c.RefreshClusterState(ctx)
			continue
		}

		resp, err := mb.CreateUser(ctx, &pb.CreateUserRequest{Name: name, RequestId: requestID})
		_ = conn.Close()
		if err == nil {
			return resp, nil
		}
		if redir := parseHeadRedirect(err); redir != "" {
			c.mu.Lock()
			c.state.Head = &pb.NodeInfo{Address: redir}
			c.mu.Unlock()
			continue
		}
		return nil, err
	}
	return nil, context.DeadlineExceeded
}

func (c *Client) CreateTopic(ctx context.Context, name, requestID string) (*pb.Topic, error) {
	if requestID == "" {
		requestID = RequestID()
	}

	for attempt := 0; attempt < 2; attempt++ {
		addr := c.headAddr()
		if addr == "" {
			if _, err := c.RefreshClusterState(ctx); err != nil {
				return nil, err
			}
			addr = c.headAddr()
		}

		conn, mb, err := c.dialMessageBoard(addr)
		if err != nil {
			_, _ = c.RefreshClusterState(ctx)
			continue
		}

		resp, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name, RequestId: requestID})
		_ = conn.Close()
		if err == nil {
			return resp, nil
		}
		if redir := parseHeadRedirect(err); redir != "" {
			c.mu.Lock()
			c.state.Head = &pb.NodeInfo{Address: redir}
			c.mu.Unlock()
			continue
		}
		return nil, err
	}
	return nil, context.DeadlineExceeded
}

func (c *Client) PostMessage(ctx context.Context, topicID, userID int64, text, requestID string) (*pb.Message, error) {
	if requestID == "" {
		requestID = RequestID()
	}

	for attempt := 0; attempt < 2; attempt++ {
		addr := c.headAddr()
		if addr == "" {
			if _, err := c.RefreshClusterState(ctx); err != nil {
				return nil, err
			}
			addr = c.headAddr()
		}

		conn, mb, err := c.dialMessageBoard(addr)
		if err != nil {
			_, _ = c.RefreshClusterState(ctx)
			continue
		}

		resp, err := mb.PostMessage(ctx, &pb.PostMessageRequest{TopicId: topicID, UserId: userID, Text: text, RequestId: requestID})
		_ = conn.Close()
		if err == nil {
			return resp, nil
		}
		if redir := parseHeadRedirect(err); redir != "" {
			c.mu.Lock()
			c.state.Head = &pb.NodeInfo{Address: redir}
			c.mu.Unlock()
			continue
		}
		return nil, err
	}
	return nil, context.DeadlineExceeded
}

func (c *Client) LikeMessage(ctx context.Context, topicID, messageID, userID int64, requestID string) (*pb.Message, error) {
	if requestID == "" {
		requestID = RequestID()
	}

	for attempt := 0; attempt < 2; attempt++ {
		addr := c.headAddr()
		if addr == "" {
			if _, err := c.RefreshClusterState(ctx); err != nil {
				return nil, err
			}
			addr = c.headAddr()
		}

		conn, mb, err := c.dialMessageBoard(addr)
		if err != nil {
			_, _ = c.RefreshClusterState(ctx)
			continue
		}

		resp, err := mb.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topicID, MessageId: messageID, UserId: userID, RequestId: requestID})
		_ = conn.Close()
		if err == nil {
			return resp, nil
		}
		if redir := parseHeadRedirect(err); redir != "" {
			c.mu.Lock()
			c.state.Head = &pb.NodeInfo{Address: redir}
			c.mu.Unlock()
			continue
		}
		return nil, err
	}
	return nil, context.DeadlineExceeded
}

func (c *Client) ListTopics(ctx context.Context) (*pb.ListTopicsResponse, error) {
	addr := c.tailAddr()
	if addr == "" {
		if _, err := c.RefreshClusterState(ctx); err != nil {
			return nil, err
		}
		addr = c.tailAddr()
	}

	conn, mb, err := c.dialMessageBoard(addr)
	if err != nil {
		if _, err2 := c.RefreshClusterState(ctx); err2 != nil {
			return nil, err
		}
		addr = c.tailAddr()
		conn, mb, err = c.dialMessageBoard(addr)
		if err != nil {
			return nil, err
		}
	}
	defer func() { _ = conn.Close() }()

	return mb.ListTopics(ctx, &emptypb.Empty{})
}

func (c *Client) GetMessages(ctx context.Context, topicID int64, fromMessageID int64, limit int32) (*pb.GetMessagesResponse, error) {
	addr := c.tailAddr()
	if addr == "" {
		if _, err := c.RefreshClusterState(ctx); err != nil {
			return nil, err
		}
		addr = c.tailAddr()
	}

	conn, mb, err := c.dialMessageBoard(addr)
	if err != nil {
		if _, err2 := c.RefreshClusterState(ctx); err2 != nil {
			return nil, err
		}
		addr = c.tailAddr()
		conn, mb, err = c.dialMessageBoard(addr)
		if err != nil {
			return nil, err
		}
	}
	defer func() { _ = conn.Close() }()

	return mb.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topicID, FromMessageId: fromMessageID, Limit: limit})
}
