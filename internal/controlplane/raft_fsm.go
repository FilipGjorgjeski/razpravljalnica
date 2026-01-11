package controlplane

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/FilipGjorgjeski/razpravljalnica/protos"
	"github.com/hashicorp/raft"
)

// RAFT Finite State Machine which applies state?
type controlPlaneFSM struct {
	server *Server
}

func (fsm *controlPlaneFSM) LogError(err error) {
	fsm.Log(fmt.Sprintf("ERROR: %s", err))
}

func (fsm *controlPlaneFSM) Log(msg string) {
	log.Printf("[raft-fsm] %s", msg)
}

// Consensus reached, this log was accepted and should be applied
func (fsm *controlPlaneFSM) Apply(logEntry *raft.Log) interface{} {
	cmd, err := UnmarshalCommand(logEntry.Data)
	if err != nil {
		fsm.LogError(err)
		return err
	}

	switch cmd.Type {
	case CommandHeartbeat:
		return fsm.applyHeartbeat(cmd.Data)
	case CommandAddNode:
		return fsm.applyAddNode(cmd.Data)
	case CommandActivateNode:
		return fsm.applyActivateNode(cmd.Data)
	default:
		err := fmt.Errorf("Unknown command type: %d", cmd.Type)
		fsm.LogError(err)
		return err
	}
}

func (fsm *controlPlaneFSM) applyHeartbeat(data []byte) interface{} {
	var payload HeartbeatCommand
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	fsm.server.mu.Lock()
	defer fsm.server.mu.Unlock()

	nodeID := payload.NodeID
	address := payload.Address
	now := time.Now()

	rt := fsm.ensureNodeRuntime(nodeID)

	rt.info = &pb.NodeInfo{NodeId: nodeID, Address: address}
	rt.status = payload.Status
	rt.lastSeenAt = now

	fsm.Log(fmt.Sprintf("heartbeat node=%s addr=%s applied=%d committed=%d", nodeID, address, rt.status.GetLastApplied(), rt.status.GetLastCommitted()))

	// Auto promote pending nodes
	fsm.server.promotePendingLocked(now)
	return nil
}

func (fsm *controlPlaneFSM) ensureNodeRuntime(nodeID string) *nodeRuntime {
	rt, ok := fsm.server.nodes[nodeID]
	if !ok || rt == nil {
		rt = &nodeRuntime{}
		fsm.server.nodes[nodeID] = rt
	}

	return rt
}

func (fsm *controlPlaneFSM) applyAddNode(data []byte) interface{} {
	var payload AddNodeCommand
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	fsm.server.mu.Lock()
	defer fsm.server.mu.Unlock()

	nodeID := payload.NodeID
	address := payload.Address

	// Is node already in chain?
	if fsm.isNodeInChain(nodeID) {
		fsm.Log(fmt.Sprintf("add node: already in chain node=%s", nodeID))
		return nil
	}

	// Is node pending?
	if _, ok := fsm.server.pending[nodeID]; ok {
		fsm.Log(fmt.Sprintf("add node: already pending node=%s", nodeID))
		return nil
	}

	// Make node be pending
	fsm.server.pending[nodeID] = &pb.NodeInfo{NodeId: nodeID, Address: address}
	fsm.Log(fmt.Sprintf("add node: added to pending pending node=%s addr=%s", nodeID, address))

	rt := fsm.ensureNodeRuntime(nodeID)

	rt.info = &pb.NodeInfo{NodeId: nodeID, Address: address}
	return nil
}

func (fsm *controlPlaneFSM) isNodeInChain(nodeID string) bool {
	for _, existing := range fsm.server.cfg.ChainOrder {
		if existing == nodeID {
			return true
		}
	}
	return false
}

func (fsm *controlPlaneFSM) applyActivateNode(data []byte) interface{} {
	var payload ActivateNodeCommand
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	fsm.server.mu.Lock()
	defer fsm.server.mu.Unlock()

	nodeID := payload.NodeID

	if fsm.isNodeInChain(nodeID) {
		log.Printf("activate node: already active node=%s", nodeID)
		return nil
	}

	// Append node to chain
	fsm.server.cfg.ChainOrder = append(fsm.server.cfg.ChainOrder, nodeID)
	delete(fsm.server.pending, nodeID)

	rt := fsm.ensureNodeRuntime(nodeID)

	info := fsm.server.pending[nodeID]
	if info != nil {
		rt.info = info
	}
	rt.lastSeenAt = time.Now()

	fsm.Log(fmt.Sprintf("activate node: activated node=%s chain=%v", nodeID, fsm.server.cfg.ChainOrder))

	return nil
}

// RAFT snapshot state
func (fsm *controlPlaneFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.server.mu.Lock()
	defer fsm.server.mu.Unlock()

	fsm.Log(fmt.Sprintf("creating snapshot configVer=%d chain=%v", fsm.server.configVer, fsm.server.cfg.ChainOrder))

	data := snapshotData{
		Nodes:      fsm.server.nodes,
		ChainOrder: fsm.server.cfg.ChainOrder,
		Pending:    fsm.server.pending,
		ConfigVer:  fsm.server.configVer,
		LastSig:    fsm.server.lastSig,
		Alive:      fsm.server.alive,
	}

	snapshot, err := NewFSMSnapshot(data)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// RAFT snapshot restore
func (fsm *controlPlaneFSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	snapshot, err := UnmarshalSnapshot(data)
	if err != nil {
		return err
	}

	// Overwrite state
	fsm.server.mu.Lock()
	defer fsm.server.mu.Unlock()

	fsm.server.nodes = snapshot.Nodes
	fsm.server.cfg.ChainOrder = snapshot.ChainOrder
	fsm.server.pending = snapshot.Pending
	fsm.server.configVer = snapshot.ConfigVer
	fsm.server.lastSig = snapshot.LastSig
	fsm.server.alive = snapshot.Alive

	fsm.Log(fmt.Sprintf("restored from snapshot configVer=%d chain=%v", fsm.server.configVer, fsm.server.cfg.ChainOrder))

	return nil
}
