package storage

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrPermissionDenied = errors.New("permission denied")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrUnauthorized     = errors.New("unauthorized")
)

type OpType int

const (
	OpPost   OpType = 0
	OpLike   OpType = 1
	OpDelete OpType = 2
	OpUpdate OpType = 3
)

// Basic domain model stored in-memory.
type User struct {
	ID        int64
	Name      string
	CreatedAt time.Time
}

type Topic struct {
	ID        int64
	Name      string
	CreatedAt time.Time
}

type Message struct {
	ID        int64
	TopicID   int64
	UserID    int64
	Text      string
	CreatedAt time.Time
	Likes     int32
	Deleted   bool // allows soft delete, simpler and more efficient than deleting from byTopic
}

// Event is an append-only entry describing changes.
type Event struct {
	SequenceNumber int64
	Op             OpType
	Message        Message
	EventAt        time.Time
}

type SubscriptionToken struct {
	Token    string
	UserID   int64
	TopicIDs []int64
	IssuedAt time.Time
}

type Storage struct {
	mu sync.RWMutex

	users    map[int64]User
	topics   map[int64]Topic
	messages map[int64]Message
	byTopic  map[int64][]int64            // topicID -> ordered message IDs
	likes    map[int64]map[int64]struct{} // messageID -> set(userID)
	events   []Event

	nextUserID      int64
	nextTopicID     int64
	nextMessageID   int64
	nextSequenceNum int64

	subTokens map[string]SubscriptionToken // token -> data
}

func New() *Storage {
	return &Storage{
		users:           map[int64]User{},
		topics:          map[int64]Topic{},
		messages:        map[int64]Message{},
		byTopic:         map[int64][]int64{},
		likes:           map[int64]map[int64]struct{}{},
		events:          make([]Event, 0, 128),
		subTokens:       map[string]SubscriptionToken{},
		nextUserID:      1,
		nextTopicID:     1,
		nextMessageID:   1,
		nextSequenceNum: 1,
	}
}

func (s *Storage) CreateUser(name string) (User, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return User{}, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// uniqueness by name.
	for _, u := range s.users {
		if u.Name == name {
			return User{}, ErrAlreadyExists
		}
	}

	user := User{ID: s.nextUserID, Name: name, CreatedAt: time.Now().UTC()}
	s.nextUserID++
	s.users[user.ID] = user
	return user, nil
}

func (s *Storage) CreateTopic(name string) (Topic, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Topic{}, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range s.topics {
		if t.Name == name {
			return Topic{}, ErrAlreadyExists
		}
	}

	topic := Topic{ID: s.nextTopicID, Name: name, CreatedAt: time.Now().UTC()}
	s.nextTopicID++
	s.topics[topic.ID] = topic
	return topic, nil
}

func (s *Storage) ListTopics() []Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]Topic, 0, len(s.topics))
	for _, t := range s.topics {
		res = append(res, t)
	}
	sort.Slice(res, func(i, j int) bool { return res[i].ID < res[j].ID })
	return res
}

func (s *Storage) PostMessage(topicID, userID int64, text string) (Message, Event, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return Message{}, Event{}, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.topics[topicID]; !ok {
		return Message{}, Event{}, ErrNotFound
	}
	if _, ok := s.users[userID]; !ok {
		return Message{}, Event{}, ErrNotFound
	}

	msg := Message{
		ID:        s.nextMessageID,
		TopicID:   topicID,
		UserID:    userID,
		Text:      text,
		CreatedAt: time.Now().UTC(),
		Likes:     0,
		Deleted:   false,
	}
	s.nextMessageID++
	s.messages[msg.ID] = msg
	s.byTopic[topicID] = append(s.byTopic[topicID], msg.ID)

	ev := s.appendEventLocked(OpPost, msg)
	return msg, ev, nil
}

func (s *Storage) UpdateMessage(topicID, userID, messageID int64, newText string) (Message, Event, error) {
	newText = strings.TrimSpace(newText)
	if newText == "" {
		return Message{}, Event{}, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.topics[topicID]; !ok {
		return Message{}, Event{}, ErrNotFound
	}
	if _, ok := s.users[userID]; !ok {
		return Message{}, Event{}, ErrNotFound
	}

	msg, ok := s.messages[messageID]
	if !ok {
		return Message{}, Event{}, ErrNotFound
	}
	if msg.Deleted {
		return Message{}, Event{}, ErrNotFound
	}
	if msg.TopicID != topicID {
		return Message{}, Event{}, ErrNotFound
	}
	if msg.UserID != userID {
		return Message{}, Event{}, ErrPermissionDenied
	}

	msg.Text = newText
	s.messages[messageID] = msg
	ev := s.appendEventLocked(OpUpdate, msg)
	return msg, ev, nil
}

func (s *Storage) DeleteMessage(topicID, userID, messageID int64) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[messageID]
	if !ok {
		return Event{}, ErrNotFound
	}
	if msg.Deleted {
		return Event{}, ErrNotFound
	}
	if msg.TopicID != topicID {
		return Event{}, ErrNotFound
	}
	if msg.UserID != userID {
		return Event{}, ErrPermissionDenied
	}

	msg.Deleted = true
	msg.Text = "" // redact
	s.messages[messageID] = msg
	ev := s.appendEventLocked(OpDelete, msg)
	return ev, nil
}

func (s *Storage) LikeMessage(topicID, messageID, userID int64) (Message, Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[messageID]
	if !ok {
		return Message{}, Event{}, ErrNotFound
	}
	if msg.Deleted {
		return Message{}, Event{}, ErrNotFound
	}
	if msg.TopicID != topicID {
		return Message{}, Event{}, ErrNotFound
	}

	set, ok := s.likes[messageID]
	if !ok { // first like for this message
		set = map[int64]struct{}{}
		s.likes[messageID] = set
	}
	if _, already := set[userID]; already {
		// this user has already liked the message: return current message without new like event
		return msg, Event{}, nil
	}
	set[userID] = struct{}{}

	msg.Likes++
	s.messages[messageID] = msg

	ev := s.appendEventLocked(OpLike, msg)
	return msg, ev, nil
}

func (s *Storage) GetMessages(topicID, fromMessageID int64, limit int32) ([]Message, error) {
	if topicID <= 0 {
		return nil, ErrInvalidArgument
	}
	if fromMessageID < 0 {
		return nil, ErrInvalidArgument
	}
	if limit < 0 {
		return nil, ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.topics[topicID]; !ok {
		return nil, ErrNotFound
	}

	ids := s.byTopic[topicID]
	res := make([]Message, 0, 32)
	for _, id := range ids {
		if id < fromMessageID {
			continue
		}
		msg := s.messages[id]
		if msg.Deleted {
			continue
		}
		res = append(res, msg)
		if limit > 0 && int32(len(res)) >= limit { // we treat limit=0 as unlimited (return all messages)
			break
		}
	}
	return res, nil
}

func (s *Storage) appendEventLocked(op OpType, msg Message) Event {
	ev := Event{
		SequenceNumber: s.nextSequenceNum,
		Op:             op,
		Message:        msg,
		EventAt:        time.Now().UTC(),
	}
	s.nextSequenceNum++
	s.events = append(s.events, ev)
	return ev
}
