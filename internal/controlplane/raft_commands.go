package controlplane

import (
	"encoding/json"
	"time"

	protos "github.com/FilipGjorgjeski/razpravljalnica/protos"
)

// CommandType specifies all commands that can be passed through the RAFT
type CommandType int

const (
	CommandHeartbeat CommandType = iota
	CommandAddNode
	CommandActivateNode
)

type Command struct {
	Type      CommandType `json:"type"`
	Data      []byte      `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// Server node related commands

type HeartbeatCommand struct {
	NodeID  string             `json:"node_id"`
	Address string             `json:"address"`
	Status  *protos.NodeStatus `json:"status"`
}

type AddNodeCommand struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type ActivateNodeCommand struct {
	NodeID string `json:"node_id"`
}

func (cmd Command) Marshal() ([]byte, error) {
	return json.Marshal(cmd)
}

func UnmarshalCommand(data []byte) (Command, error) {
	var cmd Command

	err := json.Unmarshal(data, &cmd)
	if err != nil {
		return Command{}, err
	}

	return cmd, nil
}

func NewHeartbeatCommand(nodeID, address string, status *protos.NodeStatus) (Command, error) {
	payload := HeartbeatCommand{
		NodeID:  nodeID,
		Address: address,
		Status:  status,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}

	cmd := Command{
		Type:      CommandHeartbeat,
		Data:      payloadData,
		Timestamp: time.Now(),
	}
	return cmd, nil
}

func NewAddNodeCommand(nodeID, address string) (Command, error) {
	payload := AddNodeCommand{
		NodeID:  nodeID,
		Address: address,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}

	cmd := Command{
		Type:      CommandAddNode,
		Data:      data,
		Timestamp: time.Now(),
	}
	return cmd, nil
}

func NewActivateNodeCommand(nodeID string) (Command, error) {
	payload := ActivateNodeCommand{
		NodeID: nodeID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}
	return Command{
		Type:      CommandActivateNode,
		Data:      data,
		Timestamp: time.Now(),
	}, nil
}
