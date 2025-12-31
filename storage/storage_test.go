package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateUser_TrimsAndUniqByName(t *testing.T) {
	s := New()

	u, err := s.CreateUser("  Filentie  ")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}
	if u.ID != 1 {
		t.Fatalf("expected ID=1, got %d", u.ID)
	}
	if u.Name != "Filentie" {
		t.Fatalf("expected Name=Filentie, got %q", u.Name)
	}

	if _, err := s.CreateUser("Filentie"); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	if _, err := s.CreateUser("   "); err != ErrInvalidArgument {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCreateTopic_ListTopicsSortedByID(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		s := New()

		_, err := s.CreateTopic("b")
		assert.NoError(t, err)
		_, err = s.CreateTopic("a")
		assert.NoError(t, err)
		_, err = s.CreateTopic("c")
		assert.NoError(t, err)

		topics := s.ListTopics()
		assert.Len(t, topics, 3)
		assert.EqualValues(t, 1, topics[0].ID)
		assert.EqualValues(t, 2, topics[1].ID)
		assert.EqualValues(t, 3, topics[2].ID)
	})

	t.Run("already-exists", func(t *testing.T) {
		s := New()
		_, err := s.CreateTopic("a")
		assert.NoError(t, err)

		_, err = s.CreateTopic("a")
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("invalid-argument", func(t *testing.T) {
		s := New()
		_, err := s.CreateTopic("  ")
		assert.ErrorIs(t, err, ErrInvalidArgument)
	})
}

func TestPostUpdateDeleteLikeAndGetMessages(t *testing.T) {
	s := New()

	owner, err := s.CreateUser("owner")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}
	intruder, err := s.CreateUser("intruder")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}
	liker, err := s.CreateUser("liker")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}

	if _, _, err := s.PostMessage(1, owner.ID, "hello"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for missing topic, got %v", err)
	}
	if _, _, err := s.PostMessage(1, owner.ID, "   "); err != ErrInvalidArgument {
		t.Fatalf("expected ErrInvalidArgument for empty text, got %v", err)
	}

	topic, err := s.CreateTopic("general")
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	m1, ev1, err := s.PostMessage(topic.ID, owner.ID, "  one ")
	if err != nil {
		t.Fatalf("PostMessage error: %v", err)
	}
	if m1.ID != 1 || m1.TopicID != topic.ID || m1.UserID != owner.ID || m1.Text != "one" {
		t.Fatalf("unexpected message: %+v", m1)
	}
	if ev1.SequenceNumber != 1 || ev1.Op != OpPost {
		t.Fatalf("unexpected event: %+v", ev1)
	}

	m2, _, err := s.PostMessage(topic.ID, owner.ID, "two")
	if err != nil {
		t.Fatalf("PostMessage error: %v", err)
	}
	if m2.ID != 2 {
		t.Fatalf("expected second message ID=2, got %d", m2.ID)
	}

	if _, _, err := s.UpdateMessage(topic.ID, intruder.ID, m1.ID, "x"); err != ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
	if _, _, err := s.UpdateMessage(topic.ID, owner.ID, m1.ID, "   "); err != ErrInvalidArgument {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
	updated, evUp, err := s.UpdateMessage(topic.ID, owner.ID, m1.ID, "edited")
	if err != nil {
		t.Fatalf("UpdateMessage error: %v", err)
	}
	if updated.Text != "edited" || evUp.Op != OpUpdate {
		t.Fatalf("unexpected update result: msg=%+v ev=%+v", updated, evUp)
	}

	liked, evLike, err := s.LikeMessage(topic.ID, m1.ID, liker.ID)
	if err != nil {
		t.Fatalf("LikeMessage error: %v", err)
	}
	if liked.Likes != 1 || evLike.Op != OpLike {
		t.Fatalf("unexpected like result: msg=%+v ev=%+v", liked, evLike)
	}
	liked2, evLike2, err := s.LikeMessage(topic.ID, m1.ID, liker.ID)
	if err != nil {
		t.Fatalf("LikeMessage error: %v", err)
	}
	if liked2.Likes != 1 {
		t.Fatalf("expected likes to remain 1 on duplicate like, got %d", liked2.Likes)
	}
	if evLike2.SequenceNumber != 0 {
		t.Fatalf("expected no new event on duplicate like, got seq=%d", evLike2.SequenceNumber)
	}

	if _, err := s.DeleteMessage(topic.ID, intruder.ID, m1.ID); err != ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
	if _, err := s.DeleteMessage(topic.ID, owner.ID, m1.ID); err != nil {
		t.Fatalf("DeleteMessage error: %v", err)
	}

	msgs, err := s.GetMessages(topic.ID, 0, 0)
	if err != nil {
		t.Fatalf("GetMessages error: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != m2.ID {
		t.Fatalf("expected only message ID=%d, got %+v", m2.ID, msgs)
	}

	msgs, err = s.GetMessages(topic.ID, 0, 1)
	if err != nil {
		t.Fatalf("GetMessages error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message with limit=1, got %d", len(msgs))
	}
}

func TestEventSequenceIsMonotonic(t *testing.T) {
	s := New()
	owner, err := s.CreateUser("owner")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}
	liker, err := s.CreateUser("liker")
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}

	topic, err := s.CreateTopic("general")
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	m, ev1, err := s.PostMessage(topic.ID, owner.ID, "a")
	if err != nil {
		t.Fatalf("PostMessage error: %v", err)
	}
	_, ev2, err := s.UpdateMessage(topic.ID, owner.ID, m.ID, "b")
	if err != nil {
		t.Fatalf("UpdateMessage error: %v", err)
	}
	_, ev3, err := s.LikeMessage(topic.ID, m.ID, liker.ID)
	if err != nil {
		t.Fatalf("LikeMessage error: %v", err)
	}
	if _, err := s.DeleteMessage(topic.ID, owner.ID, m.ID); err != nil {
		t.Fatalf("DeleteMessage error: %v", err)
	}

	if ev1.SequenceNumber != 1 || ev2.SequenceNumber != 2 || ev3.SequenceNumber != 3 {
		t.Fatalf("expected seq 1,2,3 got %d,%d,%d", ev1.SequenceNumber, ev2.SequenceNumber, ev3.SequenceNumber)
	}
}
