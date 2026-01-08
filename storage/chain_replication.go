package storage

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *Storage) ChainStatus() (applied, committed int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedSeq, s.committedSeq
}

func (s *Storage) ChainLookupRequest(requestID string) (RequestRecord, bool) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return RequestRecord{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.requests[requestID]
	return r, ok
}

func (s *Storage) ChainEffect(seq int64) (Effect, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ef, ok := s.effects[seq]
	return ef, ok
}

func (s *Storage) ChainEntriesFrom(fromSeq int64, limit int32) []LogEntry {
	if fromSeq <= 0 {
		fromSeq = 1
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.logEntries) == 0 {
		return nil
	}
	// logEntries are stored in seq order.
	start := 0
	for start < len(s.logEntries) && s.logEntries[start].Seq < fromSeq {
		start++
	}
	end := len(s.logEntries)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}
	res := make([]LogEntry, end-start)
	copy(res, s.logEntries[start:end])
	return res
}

func (s *Storage) ChainCommitUpTo(seq int64) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seq > s.appliedSeq {
		seq = s.appliedSeq
	}
	if seq <= s.committedSeq {
		return nil, nil
	}

	newEvents := make([]Event, 0, 8)
	for i := s.committedSeq + 1; i <= seq; i++ {
		ef, ok := s.effects[i]
		if !ok {
			continue
		}
		if !ef.Broadcast {
			continue
		}
		newEvents = append(newEvents, Event{
			SequenceNumber: i,
			Op:             ef.Op,
			Message:        ef.Message,
			EventAt:        ef.At,
		})
	}
	// Append committed events to the durable event list for backlog replay.
	// NOTE: seq may have gaps in events (e.g., duplicate like is a no-op).
	s.events = append(s.events, newEvents...)
	s.committedSeq = seq
	return newEvents, nil
}

func (s *Storage) chainReserveLogSeqLocked() int64 {
	seq := s.nextLogSeq
	s.nextLogSeq++
	return seq
}

func (s *Storage) ChainHeadCreateUser(name, requestID string, at time.Time) (LogEntry, Effect, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return LogEntry{}, Effect{}, ErrInvalidArgument
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	for _, u := range s.users {
		if u.Name == name {
			return LogEntry{}, Effect{}, ErrAlreadyExists
		}
	}

	uid := s.nextUserID
	seq := s.chainReserveLogSeqLocked()
	s.nextUserID++

	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogCreateUser, UserID: uid, Name: name}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadCreateTopic(name, requestID string, at time.Time) (LogEntry, Effect, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return LogEntry{}, Effect{}, ErrInvalidArgument
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	for _, t := range s.topics {
		if t.Name == name {
			return LogEntry{}, Effect{}, ErrAlreadyExists
		}
	}

	tid := s.nextTopicID
	seq := s.chainReserveLogSeqLocked()
	s.nextTopicID++

	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogCreateTopic, TopicID: tid, Name: name}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadPostMessage(topicID, userID int64, text, requestID string, at time.Time) (LogEntry, Effect, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return LogEntry{}, Effect{}, ErrInvalidArgument
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	if _, ok := s.topics[topicID]; !ok {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	if _, ok := s.users[userID]; !ok {
		return LogEntry{}, Effect{}, ErrNotFound
	}

	mid := s.nextMessageID
	seq := s.chainReserveLogSeqLocked()
	s.nextMessageID++

	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogPostMessage, TopicID: topicID, UserID: userID, MessageID: mid, Text: text}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadUpdateMessage(topicID, userID, messageID int64, text, requestID string, at time.Time) (LogEntry, Effect, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return LogEntry{}, Effect{}, ErrInvalidArgument
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	// Validate before consuming a sequence number.
	if _, ok := s.topics[topicID]; !ok {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	if _, ok := s.users[userID]; !ok {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	msg, ok := s.messages[messageID]
	if !ok || msg.Deleted || msg.TopicID != topicID {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	if msg.UserID != userID {
		return LogEntry{}, Effect{}, ErrPermissionDenied
	}

	seq := s.chainReserveLogSeqLocked()
	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogUpdateMessage, TopicID: topicID, UserID: userID, MessageID: messageID, Text: text}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadDeleteMessage(topicID, userID, messageID int64, requestID string, at time.Time) (LogEntry, Effect, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	// Validate before consuming a sequence number.
	msg, ok := s.messages[messageID]
	if !ok || msg.Deleted || msg.TopicID != topicID {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	if msg.UserID != userID {
		return LogEntry{}, Effect{}, ErrPermissionDenied
	}

	seq := s.chainReserveLogSeqLocked()
	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogDeleteMessage, TopicID: topicID, UserID: userID, MessageID: messageID}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadLikeMessage(topicID, messageID, userID int64, requestID string, at time.Time) (LogEntry, Effect, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if requestID != "" {
		if rr, ok := s.requests[requestID]; ok {
			if ef, ok := s.effects[rr.Seq]; ok {
				return LogEntry{}, ef, nil
			}
			return LogEntry{}, Effect{}, nil
		}
	}

	// Validate before consuming a sequence number.
	msg, ok := s.messages[messageID]
	if !ok || msg.Deleted || msg.TopicID != topicID {
		return LogEntry{}, Effect{}, ErrNotFound
	}
	if _, ok := s.users[userID]; !ok {
		return LogEntry{}, Effect{}, ErrNotFound
	}

	seq := s.chainReserveLogSeqLocked()
	e := LogEntry{Seq: seq, RequestID: requestID, At: at, Kind: LogLikeMessage, TopicID: topicID, UserID: userID, MessageID: messageID}
	ef, err := s.chainApplyEntryLocked(e)
	if err != nil {
		return LogEntry{}, Effect{}, err
	}
	return e, ef, nil
}

func (s *Storage) ChainHeadRegisterSubscription(userID int64, topicIDs []int64, token, assignedNodeID string, at time.Time) (LogEntry, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if userID <= 0 {
		return LogEntry{}, fmt.Errorf("register subscription: user_id invalid: %w", ErrInvalidArgument)
	}
	if len(topicIDs) == 0 {
		return LogEntry{}, fmt.Errorf("register subscription: topics required: %w", ErrInvalidArgument)
	}
	if token = strings.TrimSpace(token); token == "" {
		return LogEntry{}, fmt.Errorf("register subscription: token empty: %w", ErrInvalidArgument)
	}
	assignedNodeID = strings.TrimSpace(assignedNodeID)
	if assignedNodeID == "" {
		return LogEntry{}, fmt.Errorf("register subscription: assigned node empty: %w", ErrInvalidArgument)
	}

	if _, ok := s.users[userID]; !ok {
		return LogEntry{}, ErrNotFound
	}
	for _, tid := range topicIDs {
		if tid <= 0 {
			return LogEntry{}, fmt.Errorf("register subscription: topic id invalid: %w", ErrInvalidArgument)
		}
		if _, ok := s.topics[tid]; !ok {
			return LogEntry{}, ErrNotFound
		}
	}

	seq := s.chainReserveLogSeqLocked()
	e := LogEntry{Seq: seq, At: at, Kind: LogRegisterSubscription, UserID: userID, TopicIDs: append([]int64(nil), topicIDs...), Token: token, AssignedNodeID: assignedNodeID}
	if _, err := s.chainApplyEntryLocked(e); err != nil {
		return LogEntry{}, err
	}
	return e, nil
}

func (s *Storage) ChainApplyReplicatedEntry(e LogEntry) (Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.Seq <= 0 {
		return Effect{}, ErrInvalidArgument
	}
	if e.Seq != s.appliedSeq+1 {
		return Effect{}, ErrOutOfOrder
	}
	// Ensure we never reuse IDs after potential promotion to head.
	if e.Seq >= s.nextLogSeq {
		s.nextLogSeq = e.Seq + 1
	}
	if e.UserID >= s.nextUserID {
		s.nextUserID = e.UserID + 1
	}
	if e.TopicID >= s.nextTopicID {
		s.nextTopicID = e.TopicID + 1
	}
	if e.MessageID >= s.nextMessageID {
		s.nextMessageID = e.MessageID + 1
	}

	return s.chainApplyEntryLocked(e)
}

func (s *Storage) chainApplyEntryLocked(e LogEntry) (Effect, error) {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}

	// Compute effect and validate without mutating shared state first.
	ef := Effect{Seq: e.Seq, At: e.At}

	switch e.Kind {
	case LogCreateUser:
		u := User{ID: e.UserID, Name: e.Name, CreatedAt: e.At}
		for _, existing := range s.users {
			if existing.Name == u.Name {
				return Effect{}, ErrAlreadyExists
			}
		}
		ef.User = u

	case LogRegisterSubscription:
		// No additional validation needed here; input already validated by the head.

	case LogCreateTopic:
		t := Topic{ID: e.TopicID, Name: e.Name, CreatedAt: e.At}
		for _, existing := range s.topics {
			if existing.Name == t.Name {
				return Effect{}, ErrAlreadyExists
			}
		}
		ef.Topic = t

	case LogPostMessage:
		if _, ok := s.topics[e.TopicID]; !ok {
			return Effect{}, ErrNotFound
		}
		if _, ok := s.users[e.UserID]; !ok {
			return Effect{}, ErrNotFound
		}
		m := Message{ID: e.MessageID, TopicID: e.TopicID, UserID: e.UserID, Text: strings.TrimSpace(e.Text), CreatedAt: e.At, Likes: 0, Deleted: false}
		if m.Text == "" {
			return Effect{}, ErrInvalidArgument
		}
		ef.Broadcast = true
		ef.Op = OpPost
		ef.Message = m

	case LogUpdateMessage:
		m, ok := s.messages[e.MessageID]
		if !ok || m.Deleted {
			return Effect{}, ErrNotFound
		}
		if m.TopicID != e.TopicID {
			return Effect{}, ErrNotFound
		}
		if m.UserID != e.UserID {
			return Effect{}, ErrPermissionDenied
		}
		newText := strings.TrimSpace(e.Text)
		if newText == "" {
			return Effect{}, ErrInvalidArgument
		}
		m.Text = newText
		ef.Broadcast = true
		ef.Op = OpUpdate
		ef.Message = m

	case LogDeleteMessage:
		m, ok := s.messages[e.MessageID]
		if !ok || m.Deleted {
			return Effect{}, ErrNotFound
		}
		if m.TopicID != e.TopicID {
			return Effect{}, ErrNotFound
		}
		if m.UserID != e.UserID {
			return Effect{}, ErrPermissionDenied
		}
		m.Deleted = true
		m.Text = ""
		ef.Broadcast = true
		ef.Op = OpDelete
		ef.Message = m

	case LogLikeMessage:
		m, ok := s.messages[e.MessageID]
		if !ok || m.Deleted {
			return Effect{}, ErrNotFound
		}
		if m.TopicID != e.TopicID {
			return Effect{}, ErrNotFound
		}
		set := s.likes[m.ID]
		if set == nil {
			set = map[int64]struct{}{}
		}
		if _, already := set[e.UserID]; already {
			// idempotent no-op
			ef.Broadcast = false
			ef.Op = OpLike
			ef.Message = m
			break
		}
		m.Likes++
		ef.Broadcast = true
		ef.Op = OpLike
		ef.Message = m

	default:
		return Effect{}, ErrInvalidArgument
	}

	// Commit the state change.
	s.appliedSeq = e.Seq
	s.logEntries = append(s.logEntries, e)
	if e.RequestID != "" {
		s.requests[e.RequestID] = RequestRecord{Seq: e.Seq, UserID: e.UserID, TopicID: e.TopicID, MessageID: e.MessageID}
	}

	switch e.Kind {
	case LogCreateUser:
		s.users[ef.User.ID] = ef.User
		s.chainUserSeq[ef.User.ID] = e.Seq
	case LogCreateTopic:
		s.topics[ef.Topic.ID] = ef.Topic
		s.chainTopicSeq[ef.Topic.ID] = e.Seq
	case LogPostMessage, LogUpdateMessage, LogDeleteMessage, LogLikeMessage:
		// ef.Message is the post-change value (or a no-op snapshot for duplicate like).
		if ef.Message.ID != 0 {
			if e.Kind == LogPostMessage {
				s.byTopic[ef.Message.TopicID] = append(s.byTopic[ef.Message.TopicID], ef.Message.ID)
				if _, ok := s.likes[ef.Message.ID]; !ok {
					s.likes[ef.Message.ID] = map[int64]struct{}{}
				}
			}
			// Ensure likes map exists for like operations.
			if e.Kind == LogLikeMessage {
				set := s.likes[ef.Message.ID]
				if set == nil {
					set = map[int64]struct{}{}
					s.likes[ef.Message.ID] = set
				}
				if ef.Broadcast {
					set[e.UserID] = struct{}{}
				}
			}
			s.messages[ef.Message.ID] = ef.Message
			s.chainMessageSeq[ef.Message.ID] = e.Seq
		}
	case LogRegisterSubscription:
		uniq := uniqueSorted(e.TopicIDs)
		s.subTokens[e.Token] = SubscriptionToken{Token: e.Token, UserID: e.UserID, TopicIDs: uniq, AssignedNodeID: e.AssignedNodeID, IssuedAt: e.At}
	}

	s.effects[e.Seq] = ef
	return ef, nil
}

func (s *Storage) ChainIsDirtyMessage(messageID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seq := s.chainMessageSeq[messageID]
	return seq > 0 && seq > s.committedSeq
}

func (s *Storage) ChainCommittedSeq() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.committedSeq
}

// ChainListTopicsCommitted returns committed topics and whether a dirty (uncommitted) topic exists.
func (s *Storage) ChainListTopicsCommitted() (topics []Topic, dirty bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics = make([]Topic, 0, len(s.topics))
	for _, t := range s.topics {
		seq := s.chainTopicSeq[t.ID]
		if seq > 0 && seq <= s.committedSeq {
			topics = append(topics, t)
		} else if seq > s.committedSeq {
			dirty = true
		}
	}
	// Keep stable order for clients.
	// (We avoid importing sort here; ListTopics() already sorts for the legacy API.)
	for i := 0; i < len(topics); i++ {
		for j := i + 1; j < len(topics); j++ {
			if topics[j].ID < topics[i].ID {
				topics[i], topics[j] = topics[j], topics[i]
			}
		}
	}
	return topics, dirty
}

// ChainGetMessagesCommitted returns committed messages for a topic.
// If any message encountered is dirty (last update seq > committed seq), dirty will be true.
func (s *Storage) ChainGetMessagesCommitted(topicID, fromMessageID int64, limit int32) ([]Message, bool, error) {
	if topicID <= 0 {
		return nil, false, ErrInvalidArgument
	}
	if fromMessageID < 0 {
		return nil, false, ErrInvalidArgument
	}
	if limit < 0 {
		return nil, false, ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.topics[topicID]; !ok {
		return nil, false, ErrNotFound
	}

	ids := s.byTopic[topicID]
	res := make([]Message, 0, 32)
	dirty := false
	for _, id := range ids {
		if id < fromMessageID {
			continue
		}
		if seq := s.chainMessageSeq[id]; seq > s.committedSeq {
			dirty = true
			// Don't return potentially uncommitted state from this node.
			continue
		}
		msg := s.messages[id]
		if msg.Deleted {
			continue
		}
		res = append(res, msg)
		if limit > 0 && int32(len(res)) >= limit {
			break
		}
	}
	return res, dirty, nil
}

func (s *Storage) ChainLastAppliedSeq() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedSeq
}

var ErrOutOfOrder = errors.New("out of order")
