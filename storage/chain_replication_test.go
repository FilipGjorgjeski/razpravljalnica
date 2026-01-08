package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChainReplication_ApplyCommitAndCommittedReads(t *testing.T) {
	s := New()
	at := time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)

	_, uef, err := s.ChainHeadCreateUser("alice", "req-u1", at)
	require.NoError(t, err)
	require.NotZero(t, uef.User.ID)

	_, tef, err := s.ChainHeadCreateTopic("general", "req-t1", at)
	require.NoError(t, err)
	require.NotZero(t, tef.Topic.ID)

	entry, mef, err := s.ChainHeadPostMessage(tef.Topic.ID, uef.User.ID, "hello", "req-m1", at)
	require.NoError(t, err)
	require.NotZero(t, entry.Seq)
	require.NotZero(t, mef.Message.ID)

	// Before commit, committed-only reads should not return the uncommitted message.
	msgs, dirty, err := s.ChainGetMessagesCommitted(tef.Topic.ID, 0, 10)
	require.NoError(t, err)
	require.True(t, dirty)
	require.Len(t, msgs, 0)

	// Commit and ensure a broadcast event is produced for the post.
	newEvents, err := s.ChainCommitUpTo(entry.Seq)
	require.NoError(t, err)
	require.Len(t, newEvents, 1)
	require.Equal(t, entry.Seq, newEvents[0].SequenceNumber)
	require.Equal(t, OpPost, newEvents[0].Op)
	require.Equal(t, mef.Message.ID, newEvents[0].Message.ID)

	msgs, dirty, err = s.ChainGetMessagesCommitted(tef.Topic.ID, 0, 10)
	require.NoError(t, err)
	require.False(t, dirty)
	require.Len(t, msgs, 1)
	require.Equal(t, "hello", msgs[0].Text)

	// Ensure log entry listing works.
	entries := s.ChainEntriesFrom(2, 0)
	require.Len(t, entries, 2)
	require.Equal(t, int64(2), entries[0].Seq)
	require.Equal(t, int64(3), entries[1].Seq)
}

func TestChainReplication_IdempotentRequestLookup(t *testing.T) {
	s := New()
	at := time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)

	applied0 := s.ChainLastAppliedSeq()
	_, ef1, err := s.ChainHeadCreateUser("alice", "req-u1", at)
	require.NoError(t, err)
	require.Greater(t, s.ChainLastAppliedSeq(), applied0)

	applied1 := s.ChainLastAppliedSeq()
	entry2, ef2, err := s.ChainHeadCreateUser("alice", "req-u1", at)
	require.NoError(t, err)
	require.Equal(t, int64(0), entry2.Seq, "idempotent replay should not allocate a new log entry")
	require.Equal(t, ef1.User.ID, ef2.User.ID)
	require.Equal(t, applied1, s.ChainLastAppliedSeq(), "idempotent replay must not advance applied seq")

	rr, ok := s.ChainLookupRequest("req-u1")
	require.True(t, ok)
	require.Equal(t, ef1.Seq, rr.Seq)

	ef, ok := s.ChainEffect(rr.Seq)
	require.True(t, ok)
	require.Equal(t, ef1.User.ID, ef.User.ID)
}

func TestChainReplication_ApplyReplicatedEntryOutOfOrder(t *testing.T) {
	s := New()
	at := time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)

	// Out-of-order: first entry has Seq=2 but appliedSeq starts at 0.
	_, err := s.ChainApplyReplicatedEntry(LogEntry{Seq: 2, At: at, Kind: LogCreateUser, UserID: 1, Name: "alice"})
	require.ErrorIs(t, err, ErrOutOfOrder)

	// In-order entry Seq=1 should succeed.
	_, err = s.ChainApplyReplicatedEntry(LogEntry{Seq: 1, At: at, Kind: LogCreateUser, UserID: 1, Name: "alice"})
	require.NoError(t, err)

	// Duplicate/old (Seq=1 again) is treated as out-of-order by our strict in-order contract.
	_, err = s.ChainApplyReplicatedEntry(LogEntry{Seq: 1, At: at, Kind: LogCreateUser, UserID: 1, Name: "alice"})
	require.ErrorIs(t, err, ErrOutOfOrder)
}

func TestChainReplication_LikeIsIdempotentNoBroadcast(t *testing.T) {
	s := New()
	at := time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC)

	_, uef, err := s.ChainHeadCreateUser("alice", "req-u1", at)
	require.NoError(t, err)
	_, tef, err := s.ChainHeadCreateTopic("general", "req-t1", at)
	require.NoError(t, err)
	_, mef, err := s.ChainHeadPostMessage(tef.Topic.ID, uef.User.ID, "hello", "req-m1", at)
	require.NoError(t, err)

	like1, ef1, err := s.ChainHeadLikeMessage(tef.Topic.ID, mef.Message.ID, uef.User.ID, "req-like-1", at)
	require.NoError(t, err)
	require.True(t, ef1.Broadcast)
	require.Equal(t, int32(1), ef1.Message.Likes)

	like2, ef2, err := s.ChainHeadLikeMessage(tef.Topic.ID, mef.Message.ID, uef.User.ID, "req-like-2", at)
	require.NoError(t, err)
	require.False(t, ef2.Broadcast, "second like by same user should be a no-op")
	require.Equal(t, int32(1), ef2.Message.Likes, "likes must not increment on duplicate like")
	require.Greater(t, like2.Seq, like1.Seq, "even no-op likes are still logged (new Seq)")

	newEvents, err := s.ChainCommitUpTo(like2.Seq)
	require.NoError(t, err)
	// Expect exactly one like event (the first like); the second is Broadcast=false.
	foundLikes := 0
	for _, ev := range newEvents {
		if ev.Op == OpLike {
			foundLikes++
		}
	}
	require.Equal(t, 1, foundLikes)

	msgs, dirty, err := s.ChainGetMessagesCommitted(tef.Topic.ID, 0, 10)
	require.NoError(t, err)
	require.False(t, dirty)
	require.Len(t, msgs, 1)
	require.Equal(t, int32(1), msgs[0].Likes)
}
