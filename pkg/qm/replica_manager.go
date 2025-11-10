package qm

import (
	"fmt"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pjavanrood/tinygraph/internal/config"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

func getReplicaIdFromLeaderID(leaderID string) (int, error) {
	// shard-<shardID>-replica-<replicaID> format
	parts := strings.Split(leaderID, "-")
	if len(parts) != 4 {
		return 0, fmt.Errorf("invalid leader ID format: %s", leaderID)
	}
	replicaID, err := strconv.Atoi(parts[3])
	if err != nil {
		return 0, err
	}
	return int(replicaID), nil
}

// ReplicaManager is an interface for managing replica leadership information
type ReplicaManager interface {
	Start()
	GetLeaderID(shardID int) (int, error)
}

// PushBasedReplicaManager waits for the leader ID to be pushed via notifications
type PushBasedReplicaManager struct {
	cfg              *config.Config
	mapShardToLeader map[int]int
	mu               sync.Mutex
}

func (rm *PushBasedReplicaManager) Start() {}

func (rm *PushBasedReplicaManager) GetLeaderID(shardID int) (int, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.mapShardToLeader[shardID], nil
}

func (rm *PushBasedReplicaManager) NotifyLeaderIDUpdate(req *rpcTypes.NotifyLeaderIDUpdateRequest, resp *rpcTypes.NotifyLeaderIDUpdateResponse) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	replicaID, err := getReplicaIdFromLeaderID(req.LeaderID)
	if err != nil {
		return fmt.Errorf("failed to get replica ID from leader ID: %w", err)
	}
	rm.mapShardToLeader[req.ShardID] = replicaID
	return nil
}

// NewPushBasedReplicaManager creates a new ReplicaManager implementation
func NewPushBasedReplicaManager(cfg *config.Config) ReplicaManager {
	return &PushBasedReplicaManager{
		cfg:              cfg,
		mapShardToLeader: make(map[int]int),
		mu:               sync.Mutex{},
	}
}

// ------------------------------------------------------------

const COLLECT_LEADER_IDS_INTERVAL = 1 * time.Second

// PollBasedReplicaManager actively polls the replicas to get the leader ID
type PollBasedReplicaManager struct {
	cfg              *config.Config
	mapShardToLeader map[int]int
	mu               sync.Mutex
}

// NewPollBasedReplicaManager creates a new ReplicaManager implementation
func NewPollBasedReplicaManager(cfg *config.Config) ReplicaManager {
	return &PollBasedReplicaManager{
		cfg:              cfg,
		mapShardToLeader: make(map[int]int),
		mu:               sync.Mutex{},
	}
}

func (rm *PollBasedReplicaManager) getLeaderID(shardID int) (int, error) {
	for _, replicaConfig := range rm.cfg.Shards[shardID].Replicas {
		client, err := rpc.Dial("tcp", replicaConfig.GetRPCAddress())
		if err != nil {
			log.Printf("Failed to dial replica %s: %v", replicaConfig.GetRPCAddress(), err)
			continue
		}
		defer client.Close()
		var leaderIDResponse rpcTypes.RaftLeadershipResponse
		err = client.Call("Shard.GetLeaderID", &rpcTypes.RaftLeadershipRequest{}, &leaderIDResponse)
		if err != nil {
			log.Printf("Failed to get leader ID from replica %s: %v", replicaConfig.GetRPCAddress(), err)
			continue
		}

		replicaID, err := getReplicaIdFromLeaderID(string(leaderIDResponse))
		if err != nil {
			log.Printf("Failed to get replica ID from leader ID: %v", err)
			continue
		}
		rm.mapShardToLeader[shardID] = replicaID
		return replicaID, nil
	}
	return 0, fmt.Errorf("no leader found for shard %d", shardID)
}

func (rm *PollBasedReplicaManager) collectLeaderIDs() error {
	for _, shardConfig := range rm.cfg.Shards {
		leaderID, err := rm.getLeaderID(shardConfig.ID)
		if err != nil {
			log.Printf("Failed to get leader ID for shard %d: %v", shardConfig.ID, err)
			continue
		}
		rm.mu.Lock()
		defer rm.mu.Unlock()
		rm.mapShardToLeader[shardConfig.ID] = leaderID
		log.Printf("Collected leader ID for shard %d: %d", shardConfig.ID, leaderID)
	}
	return nil
}

func (rm *PollBasedReplicaManager) GetLeaderID(shardID int) (int, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.mapShardToLeader[shardID], nil
}

func (rm *PollBasedReplicaManager) Start() {
	go func() {
		for {
			err := rm.collectLeaderIDs()
			if err != nil {
				log.Printf("Failed to collect leader IDs: %v", err)
			}
			time.Sleep(COLLECT_LEADER_IDS_INTERVAL)
		}
	}()
}
