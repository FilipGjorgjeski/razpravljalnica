package storage

import (
	"time"
)

type LogKind int

const (
	LogUnknown LogKind = iota
	LogCreateUser
	LogCreateTopic
	LogPostMessage
	LogUpdateMessage
	LogDeleteMessage
	LogLikeMessage
	LogRegisterSubscription
)

type LogEntry struct {
	Seq       int64
	RequestID string
	At        time.Time
	Kind      LogKind

	UserID         int64
	TopicID        int64
	MessageID      int64
	TopicIDs       []int64
	Token          string
	AssignedNodeID string
	Name           string
	Text           string
}

type Effect struct {
	Seq int64
	At  time.Time

	// Broadcast indicates whether this entry should be broadcast to subscribers.
	Broadcast bool
	Op        OpType
	Message   Message

	// Response snapshots for the original request (idempotency).
	User  User
	Topic Topic
}

type RequestRecord struct {
	Seq int64
	// IDs are enough to reconstruct a response from state.
	UserID    int64
	TopicID   int64
	MessageID int64
}
