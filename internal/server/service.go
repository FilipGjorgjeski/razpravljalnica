package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Services struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer

	store         *storage.Storage
	hub           *Hub
	nodeID        string
	advertiseAddr string
}

func (s *Services) logf(format string, args ...interface{}) {
	log.Printf("[svc %s] "+format, append([]interface{}{s.nodeID}, args...)...)
}

func NewServices(store *storage.Storage, hub *Hub, nodeID, advertiseAddr string) *Services {
	return &Services{store: store, hub: hub, nodeID: nodeID, advertiseAddr: advertiseAddr}
}

func (s *Services) Register(grpcServer *grpc.Server) {
	pb.RegisterMessageBoardServer(grpcServer, s)
	pb.RegisterControlPlaneServer(grpcServer, s)
	reflection.Register(grpcServer)
}

func ListenAndServe(listenAddr string, services *Services) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	services.Register(gs)
	return gs.Serve(lis)
}

func (s *Services) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	s.logf("create_user name=%s", req.GetName())
	u, err := s.store.CreateUser(req.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	s.logf("create_user done id=%d", u.ID)
	return &pb.User{Id: u.ID, Name: u.Name}, nil
}

func (s *Services) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	s.logf("create_topic name=%s", req.GetName())
	t, err := s.store.CreateTopic(req.GetName())
	if err != nil {
		return nil, mapErr(err)
	}
	s.logf("create_topic done id=%d", t.ID)
	return &pb.Topic{Id: t.ID, Name: t.Name}, nil
}

func (s *Services) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	s.logf("post_message topic=%d user=%d", req.GetTopicId(), req.GetUserId())
	msg, ev, err := s.store.PostMessage(req.GetTopicId(), req.GetUserId(), req.GetText())
	if err != nil {
		return nil, mapErr(err)
	}
	s.hub.Broadcast(ev)
	s.logf("post_message done id=%d seq=%d", msg.ID, ev.SequenceNumber)
	return toProtoMessage(msg, s.store.GetUser(msg.UserID).Name, false), nil
}

func (s *Services) UpdateMessage(ctx context.Context, req *pb.UpdateMessageRequest) (*pb.Message, error) {
	s.logf("update_message topic=%d id=%d user=%d", req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	msg, ev, err := s.store.UpdateMessage(req.GetTopicId(), req.GetUserId(), req.GetMessageId(), req.GetText())
	if err != nil {
		return nil, mapErr(err)
	}
	s.hub.Broadcast(ev)
	s.logf("update_message done id=%d seq=%d", msg.ID, ev.SequenceNumber)
	return toProtoMessage(msg, s.store.GetUser(msg.UserID).Name, s.store.IsMessageLikedByUser(req.MessageId, req.UserId)), nil
}

func (s *Services) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	s.logf("delete_message topic=%d id=%d user=%d", req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	ev, err := s.store.DeleteMessage(req.GetTopicId(), req.GetUserId(), req.GetMessageId())
	if err != nil {
		return nil, mapErr(err)
	}
	s.hub.Broadcast(ev)
	s.logf("delete_message done seq=%d", ev.SequenceNumber)
	return &emptypb.Empty{}, nil
}

func (s *Services) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	s.logf("like_message topic=%d id=%d user=%d", req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	msg, ev, err := s.store.LikeMessage(req.GetTopicId(), req.GetMessageId(), req.GetUserId())
	if err != nil {
		return nil, mapErr(err)
	}
	if ev.SequenceNumber != 0 { // only broadcast if like is not duplicate
		s.hub.Broadcast(ev)
	}
	s.logf("like_message done seq=%d duplicate=%v", ev.SequenceNumber, ev.SequenceNumber == 0)
	return toProtoMessage(msg, s.store.GetUser(msg.UserID).Name, true), nil
}

func (s *Services) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	tok, err := s.store.CreateSubscriptionToken(req.GetUserId(), req.GetTopicId())
	if err != nil {
		return nil, mapErr(err)
	}
	s.logf("subscription_token issued user=%d topics=%v node=%s", req.GetUserId(), req.GetTopicId(), s.nodeID)
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: tok,
		Node:           &pb.NodeInfo{NodeId: s.nodeID, Address: s.advertiseAddr},
	}, nil
}

func (s *Services) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	topics := s.store.ListTopics()
	res := make([]*pb.Topic, 0, len(topics))
	for _, t := range topics {
		res = append(res, &pb.Topic{Id: t.ID, Name: t.Name})
	}
	s.logf("list_topics count=%d", len(res))
	return &pb.ListTopicsResponse{Topics: res}, nil
}

func (s *Services) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	msgs, err := s.store.GetMessages(req.GetTopicId(), req.GetFromMessageId(), req.GetLimit())
	if err != nil {
		return nil, mapErr(err)
	}
	res := make([]*pb.Message, 0, len(msgs))
	for _, m := range msgs {
		res = append(res, toProtoMessage(m, s.store.GetUser(m.UserID).Name, s.store.IsMessageLikedByUser(m.ID, req.UserId)))
	}
	s.logf("get_messages topic=%d from=%d limit=%d returned=%d", req.GetTopicId(), req.GetFromMessageId(), req.GetLimit(), len(res))
	return &pb.GetMessagesResponse{Messages: res}, nil
}

func (s *Services) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	if len(req.GetTopicId()) == 0 {
		return status.Error(codes.InvalidArgument, "topic_id required")
	}
	if req.GetUserId() <= 0 {
		return status.Error(codes.InvalidArgument, "user_id required")
	}
	if req.GetSubscribeToken() == "" {
		return status.Error(codes.Unauthenticated, "subscribe_token required")
	}
	if err := s.store.ValidateSubscriptionToken(req.GetSubscribeToken(), req.GetUserId(), req.GetTopicId(), s.nodeID); err != nil {
		return mapErr(err)
	}
	fromMessageID := req.GetFromMessageId()
	if fromMessageID < 0 {
		return status.Error(codes.InvalidArgument, "from_message_id must be >= 0")
	}
	s.logf("subscribe start user=%d topics=%v from=%d", req.GetUserId(), req.GetTopicId(), fromMessageID)

	_, ch, remove := s.hub.Add(req.GetTopicId())
	defer remove()

	watermark := s.store.CurrentSequence()
	backlog := s.store.EventsBetween(0, watermark)
	s.logf("subscribe backlog events=%d watermark=%d", len(backlog), watermark)

	topicSet := make(map[int64]struct{}, len(req.GetTopicId()))
	for _, tid := range req.GetTopicId() {
		topicSet[tid] = struct{}{}
	}

	var streamSeq int64 = 1
	send := func(op pb.OpType, m storage.Message, at time.Time) error {
		ev := &pb.MessageEvent{
			SequenceNumber: streamSeq,
			Op:             op,
			Message:        toProtoMessage(m, s.store.GetUser(m.UserID).Name, s.store.IsMessageLikedByUser(m.ID, req.UserId)),
			EventAt:        timestamppb.New(at),
		}
		streamSeq++
		return stream.Send(ev)
	}
	for _, ev := range backlog {
		if _, ok := topicSet[ev.Message.TopicID]; !ok {
			continue
		}
		if ev.Message.ID < fromMessageID {
			continue
		}
		if err := send(toProtoOp(ev.Op), ev.Message, ev.EventAt); err != nil {
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			// Avoid duplicates already covered by backlog.
			if ev.SequenceNumber <= watermark {
				continue
			}
			if ev.Message.ID < fromMessageID {
				continue
			}
			if err := send(toProtoOp(ev.Op), ev.Message, ev.EventAt); err != nil {
				return err
			}
		}
	}
}

func (s *Services) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	n := &pb.NodeInfo{NodeId: s.nodeID, Address: s.advertiseAddr}
	s.logf("cluster_state single_node head/tail=%s", s.nodeID)
	return &pb.GetClusterStateResponse{Head: n, Tail: n}, nil
}

func toProtoMessage(m storage.Message, username string, messageLikedByUser bool) *pb.Message {
	return &pb.Message{
		Id:          m.ID,
		TopicId:     m.TopicID,
		UserId:      m.UserID,
		Username:    username,
		Text:        m.Text,
		CreatedAt:   timestamppb.New(m.CreatedAt),
		Likes:       m.Likes,
		LikedByUser: messageLikedByUser,
	}
}

func toProtoOp(op storage.OpType) pb.OpType {
	switch op {
	case storage.OpPost:
		return pb.OpType_OP_POST
	case storage.OpLike:
		return pb.OpType_OP_LIKE
	case storage.OpDelete:
		return pb.OpType_OP_DELETE
	case storage.OpUpdate:
		return pb.OpType_OP_UPDATE
	default:
		return pb.OpType_OP_POST
	}
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, storage.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, storage.ErrPermissionDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, storage.ErrUnauthorized):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("internal error: %v", err))
	}
}
