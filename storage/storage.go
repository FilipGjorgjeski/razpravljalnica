package storage

import (
	"crypto/rand"
	"encoding/base64"
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

type Event struct {
	SequenceNumber int64
	Op             OpType
	Message        Message
	EventAt        time.Time
}

type SubscriptionToken struct {
	Token          string
	UserID         int64
	TopicIDs       []int64
	AssignedNodeID string
	IssuedAt       time.Time
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
	nextLogSeq      int64

	// Replication bookkeeping (used only in chain-replication mode).
	appliedSeq   int64
	committedSeq int64

	logEntries []LogEntry
	effects    map[int64]Effect         // seq -> effect snapshot
	requests   map[string]RequestRecord // request_id -> record (for idempotency)

	// Per-record last applied log sequence (used for dirty/clean reads under pipelining).
	chainUserSeq    map[int64]int64
	chainTopicSeq   map[int64]int64
	chainMessageSeq map[int64]int64

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
		logEntries:      make([]LogEntry, 0, 128),
		effects:         map[int64]Effect{},
		requests:        map[string]RequestRecord{},
		chainUserSeq:    map[int64]int64{},
		chainTopicSeq:   map[int64]int64{},
		chainMessageSeq: map[int64]int64{},
		nextUserID:      1,
		nextTopicID:     1,
		nextMessageID:   1,
		nextSequenceNum: 1,
		appliedSeq:      0,
		committedSeq:    0,
		nextLogSeq:      1,
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

func (s *Storage) CreateSubscriptionToken(userID int64, topicIDs []int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[userID]; !ok {
		return "", ErrNotFound
	}
	if len(topicIDs) == 0 {
		return "", ErrInvalidArgument
	}
	for _, tid := range topicIDs {
		if tid <= 0 {
			return "", ErrInvalidArgument
		}
		if _, ok := s.topics[tid]; !ok {
			return "", ErrNotFound
		}
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(buf)

	uniq := uniqueSorted(topicIDs)
	s.subTokens[tok] = SubscriptionToken{
		Token:    tok,
		UserID:   userID,
		TopicIDs: uniq,
		IssuedAt: time.Now().UTC(),
	}
	return tok, nil
}

func (s *Storage) ValidateSubscriptionToken(token string, userID int64, topicIDs []int64, nodeID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st, ok := s.subTokens[token]
	if !ok {
		return ErrUnauthorized
	}
	if st.UserID != userID {
		return ErrUnauthorized
	}
	if nodeID != "" && st.AssignedNodeID != "" && st.AssignedNodeID != nodeID {
		return ErrUnauthorized
	}

	requested := uniqueSorted(topicIDs)
	if !isSubset(requested, st.TopicIDs) {
		return ErrUnauthorized
	}
	return nil
}

// CurrentSequence returns the last assigned event sequence number (0 if none).
func (s *Storage) CurrentSequence() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq := s.nextSequenceNum - 1
	if s.committedSeq > seq {
		seq = s.committedSeq
	}
	return seq
}

// EventsBetween returns events where fromExclusive < seq <= toInclusive.
func (s *Storage) EventsBetween(fromExclusive, toInclusive int64) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if toInclusive <= fromExclusive {
		return nil
	}

	start := 0
	for start < len(s.events) && s.events[start].SequenceNumber <= fromExclusive {
		start++
	}
	end := start
	for end < len(s.events) && s.events[end].SequenceNumber <= toInclusive {
		end++
	}
	res := make([]Event, end-start)
	copy(res, s.events[start:end])
	return res
}

func uniqueSorted(ids []int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	copyIDs := append([]int64(nil), ids...)
	sort.Slice(copyIDs, func(i, j int) bool { return copyIDs[i] < copyIDs[j] })
	res := make([]int64, 0, len(copyIDs))
	var last int64
	for i, id := range copyIDs {
		if i == 0 || id != last {
			res = append(res, id)
			last = id
		}
	}
	return res
}

func isSubset(lookFor, lookIn []int64) bool {
	i, j := 0, 0
	for i < len(lookFor) && j < len(lookIn) {
		if lookFor[i] == lookIn[j] {
			i++
			j++
			continue
		}
		if lookFor[i] > lookIn[j] {
			j++
			continue
		}
		return false
	}
	return i == len(lookFor)
}
