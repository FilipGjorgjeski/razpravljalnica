package controlplane

import (
	"encoding/json"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/hashicorp/raft"
)

// Snapshot of FSM state
type fsmSnapshot struct {
	data []byte
}

func NewFSMSnapshot(data snapshotData) (*fsmSnapshot, error) {
	bytes, err := data.Marshall()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{
		data: bytes,
	}, nil
}

type snapshotData struct {
	Nodes      map[string]*nodeRuntime `json:"nodes"`
	ChainOrder []string                `json:"chain_order"`
	Pending    map[string]*pb.NodeInfo `json:"pending"`
	ConfigVer  int64                   `json:"config_ver"`
	LastSig    string                  `json:"last_sig"`
	Alive      map[string]bool         `json:"alive"`
}

func (sd *snapshotData) Marshall() ([]byte, error) {
	bytes, err := json.Marshal(sd)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func UnmarshalSnapshot(data []byte) (*snapshotData, error) {
	snapshot := &snapshotData{}

	err := json.Unmarshal(data, snapshot)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {
}
