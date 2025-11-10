package shard

import (
	"net/rpc"

	"github.com/hashicorp/raft"
	rpcTypes "github.com/pjavanrood/tinygraph/pkg/rpc"
)

type LeaderObserver struct {
	shardID   int
	qmAddress string
}

func NewLeaderObserver(shardID int, qmAddress string) *LeaderObserver {
	return &LeaderObserver{
		shardID:   shardID,
		qmAddress: qmAddress,
	}
}

func (lo *LeaderObserver) notifyQueryManager(leaderID string) {
	// Make an RPC call to PushBasedReplicaManager to notify the leader ID
	client, err := rpc.Dial("tcp", lo.qmAddress)
	if err != nil {
		log.Printf("Failed to dial PushBasedReplicaManager: %v", err)
		return
	}
	defer client.Close()

	var resp rpcTypes.NotifyLeaderIDUpdateResponse
	err = client.Call("PushBasedReplicaManager.NotifyLeaderIDUpdate", &rpcTypes.NotifyLeaderIDUpdateRequest{
		ShardID:  lo.shardID,
		LeaderID: leaderID,
	}, &resp)
	if err != nil {
		log.Printf("Failed to notify leader ID update: %v", err)
		return
	}
	log.Printf("Successfully notified query manager of new leader: %s", leaderID)
}

// SetupLeaderElectionObserver sets up an observer that watches for leader election events
// and notifies the query manager when this node becomes or loses leadership
func SetupLeaderElectionObserver(r *raft.Raft, shardID int, localID raft.ServerID, qmAddress string) {
	leaderObserver := NewLeaderObserver(shardID, qmAddress)

	// Create a channel to receive leader observations
	obsChan := make(chan raft.Observation, 1)

	// Create a filter function that only passes leader observations
	filterFn := func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	}

	// Create an observer that filters for leader election events
	observer := raft.NewObserver(obsChan, false, filterFn)
	r.RegisterObserver(observer)

	// Start a goroutine to handle leader election events
	go func() {
		for obs := range obsChan {
			if leaderObs, ok := obs.Data.(raft.LeaderObservation); ok {
				leaderID := string(leaderObs.LeaderID)

				if leaderObs.LeaderID == localID {
					log.Printf("This node became the leader (ID: %s)", leaderID)
					leaderObserver.notifyQueryManager(leaderID)
				} else if leaderObs.LeaderID != "" {
					log.Printf("New leader elected: %s", leaderID)
				} else {
					log.Printf("Lost leadership, no leader currently")
				}
			}
		}
	}()
}
