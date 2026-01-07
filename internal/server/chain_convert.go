package server

import (
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func storageToPBEntry(e storage.LogEntry) *pb.LogEntry {
	out := &pb.LogEntry{
		SequenceNumber: e.Seq,
		RequestId:      e.RequestID,
		At:             timestamppb.New(e.At),
	}
	switch e.Kind {
	case storage.LogCreateUser:
		out.Op = &pb.LogEntry_CreateUser{CreateUser: &pb.CreateUserOp{UserId: e.UserID, Name: e.Name}}
	case storage.LogCreateTopic:
		out.Op = &pb.LogEntry_CreateTopic{CreateTopic: &pb.CreateTopicOp{TopicId: e.TopicID, Name: e.Name}}
	case storage.LogPostMessage:
		out.Op = &pb.LogEntry_PostMessage{PostMessage: &pb.PostMessageOp{MessageId: e.MessageID, TopicId: e.TopicID, UserId: e.UserID, Text: e.Text}}
	case storage.LogUpdateMessage:
		out.Op = &pb.LogEntry_UpdateMessage{UpdateMessage: &pb.UpdateMessageOp{TopicId: e.TopicID, UserId: e.UserID, MessageId: e.MessageID, Text: e.Text}}
	case storage.LogDeleteMessage:
		out.Op = &pb.LogEntry_DeleteMessage{DeleteMessage: &pb.DeleteMessageOp{TopicId: e.TopicID, UserId: e.UserID, MessageId: e.MessageID}}
	case storage.LogLikeMessage:
		out.Op = &pb.LogEntry_LikeMessage{LikeMessage: &pb.LikeMessageOp{TopicId: e.TopicID, MessageId: e.MessageID, UserId: e.UserID}}
	case storage.LogRegisterSubscription:
		out.Op = &pb.LogEntry_RegisterSubscription{RegisterSubscription: &pb.RegisterSubscriptionOp{Token: e.Token, UserId: e.UserID, TopicId: e.TopicIDs, AssignedNodeId: e.AssignedNodeID}}
	}
	return out
}

func pbToStorageEntry(e *pb.LogEntry) storage.LogEntry {
	out := storage.LogEntry{
		Seq:       e.GetSequenceNumber(),
		RequestID: e.GetRequestId(),
		Kind:      storage.LogUnknown,
	}
	if ts := e.GetAt(); ts != nil {
		out.At = ts.AsTime().UTC()
	}
	if out.At.IsZero() {
		out.At = time.Now().UTC()
	}

	switch op := e.GetOp().(type) {
	case *pb.LogEntry_CreateUser:
		out.Kind = storage.LogCreateUser
		out.UserID = op.CreateUser.GetUserId()
		out.Name = op.CreateUser.GetName()
	case *pb.LogEntry_CreateTopic:
		out.Kind = storage.LogCreateTopic
		out.TopicID = op.CreateTopic.GetTopicId()
		out.Name = op.CreateTopic.GetName()
	case *pb.LogEntry_PostMessage:
		out.Kind = storage.LogPostMessage
		out.MessageID = op.PostMessage.GetMessageId()
		out.TopicID = op.PostMessage.GetTopicId()
		out.UserID = op.PostMessage.GetUserId()
		out.Text = op.PostMessage.GetText()
	case *pb.LogEntry_UpdateMessage:
		out.Kind = storage.LogUpdateMessage
		out.TopicID = op.UpdateMessage.GetTopicId()
		out.UserID = op.UpdateMessage.GetUserId()
		out.MessageID = op.UpdateMessage.GetMessageId()
		out.Text = op.UpdateMessage.GetText()
	case *pb.LogEntry_DeleteMessage:
		out.Kind = storage.LogDeleteMessage
		out.TopicID = op.DeleteMessage.GetTopicId()
		out.UserID = op.DeleteMessage.GetUserId()
		out.MessageID = op.DeleteMessage.GetMessageId()
	case *pb.LogEntry_LikeMessage:
		out.Kind = storage.LogLikeMessage
		out.TopicID = op.LikeMessage.GetTopicId()
		out.UserID = op.LikeMessage.GetUserId()
		out.MessageID = op.LikeMessage.GetMessageId()
	case *pb.LogEntry_RegisterSubscription:
		out.Kind = storage.LogRegisterSubscription
		out.Token = op.RegisterSubscription.GetToken()
		out.UserID = op.RegisterSubscription.GetUserId()
		out.TopicIDs = append([]int64(nil), op.RegisterSubscription.GetTopicId()...)
		out.AssignedNodeID = op.RegisterSubscription.GetAssignedNodeId()
	}
	return out
}
