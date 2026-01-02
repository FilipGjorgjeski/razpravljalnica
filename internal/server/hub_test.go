package server

import (
	"testing"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/storage"
)

func TestHub_AddBroadcastRemove(t *testing.T) {
	h := NewHub()

	_, ch, remove := h.Add([]int64{1})
	defer remove()

	// Unrelated topic should not be delivered.
	h.Broadcast(storage.Event{Message: storage.Message{TopicID: 2}})
	select {
	case <-ch:
		t.Fatalf("unexpected event for non-subscribed topic")
	case <-time.After(50 * time.Millisecond):
		// ok
	}

	want := storage.Event{SequenceNumber: 123, Message: storage.Message{TopicID: 1, ID: 9}}
	h.Broadcast(want)
	select {
	case got := <-ch:
		if got.SequenceNumber != want.SequenceNumber || got.Message.TopicID != want.Message.TopicID || got.Message.ID != want.Message.ID {
			t.Fatalf("unexpected event: got=%+v want=%+v", got, want)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for broadcast")
	}

	// Remove should close the channel.
	remove()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel to be closed after remove")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for channel close")
	}
}
