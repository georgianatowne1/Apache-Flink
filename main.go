package main

import (
	"fmt"
	"sync"
)

// Event represents a data record with a unique sequence number.
type Event struct {
	SeqNo int
	Data  string
}

// Checkpoint represents the state of the system at a specific point.
type Checkpoint struct {
	ID           int
	SourceOffset int
}

// CompletedCheckpointStore simulates a highly available store (e.g., ZooKeeper).
type CompletedCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[int]Checkpoint
	latestID    int
}

func NewCompletedCheckpointStore() *CompletedCheckpointStore {
	return &CompletedCheckpointStore{
		checkpoints: make(map[int]Checkpoint),
		latestID:    -1,
	}
}

func (s *CompletedCheckpointStore) AddCheckpoint(cp Checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[cp.ID] = cp
	if cp.ID > s.latestID {
		s.latestID = cp.ID
	}
}

func (s *CompletedCheckpointStore) GetLatestCheckpoint() (Checkpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestID == -1 {
		return Checkpoint{}, false
	}
	return s.checkpoints[s.latestID], true
}

// SourceCoordinator manages the state and offsets of the data source.
type SourceCoordinator struct {
	currentOffset int
}

func (sc *SourceCoordinator) GetOffset() int {
	return sc.currentOffset
}

func (sc *SourceCoordinator) SetOffset(offset int) {
	sc.currentOffset = offset
}

// TwoPhaseCommitSink simulates a sink that supports exactly-once via two-phase commit.
type TwoPhaseCommitSink struct {
	mu                 sync.Mutex
	committedData      []Event
	pendingTransactions map[int][]Event // checkpointID -> list of events
}

func NewTwoPhaseCommitSink() *TwoPhaseCommitSink {
	return &TwoPhaseCommitSink{
		pendingTransactions: make(map[int][]Event),
	}
}

func (sink *TwoPhaseCommitSink) Write(event Event, checkpointID int) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.pendingTransactions[checkpointID] = append(sink.pendingTransactions[checkpointID], event)
}

func (sink *TwoPhaseCommitSink) Commit(checkpointID int) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if events, exists := sink.pendingTransactions[checkpointID]; exists {
		sink.committedData = append(sink.committedData, events...)
		delete(sink.pendingTransactions, checkpointID)
	}
}

func (sink *TwoPhaseCommitSink) Abort(checkpointID int) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	delete(sink.pendingTransactions, checkpointID)
}

// JobManager coordinates the execution, checkpointing, and recovery.
type JobManager struct {
	store             *CompletedCheckpointStore
	sourceCoordinator *SourceCoordinator
	sink              *TwoPhaseCommitSink
	nextCheckpointID  int
}

func NewJobManager(store *CompletedCheckpointStore, source *SourceCoordinator, sink *TwoPhaseCommitSink) *JobManager {
	return &JobManager{
		store:             store,
		sourceCoordinator: source,
		sink:              sink,
		nextCheckpointID:  1,
	}
}

// TriggerCheckpoint initiates a checkpoint.
func (jm *JobManager) TriggerCheckpoint() int {
	cpID := jm.nextCheckpointID
	jm.nextCheckpointID++
	return cpID
}

// CommitCheckpoint persists the checkpoint to the HA store and commits the sink transactions.
func (jm *JobManager) CommitCheckpoint(cpID int, offset int) {
	cp := Checkpoint{ID: cpID, SourceOffset: offset}
	jm.store.AddCheckpoint(cp)
	jm.sink.Commit(cpID)
}

// Failover simulates a JobManager crash and recovery.
func (jm *JobManager) Failover() {
	fmt.Println("--- JobManager Failover Initiated ---")
	// A new JobManager instance is created and recovers from the HA store.
	latestCP, exists := jm.store.GetLatestCheckpoint()
	if !exists {
		fmt.Println("No completed checkpoint found. Restoring to initial state.")
		jm.sourceCoordinator.SetOffset(0)
		return
	}

	fmt.Printf("Recovering from Checkpoint %d (Source Offset: %d)\n", latestCP.ID, latestCP.SourceOffset)
	
	// Roll back the source coordinator to the last committed checkpoint offset
	jm.sourceCoordinator.SetOffset(latestCP.SourceOffset)

	// Abort any pending transactions that were not committed in the HA store
	for cpID := latestCP.ID + 1; cpID < jm.nextCheckpointID; cpID++ {
		jm.sink.Abort(cpID)
	}
	
	// Reset the next checkpoint ID to resume from the recovered checkpoint
	jm.nextCheckpointID = latestCP.ID + 1
}

func main() {
	// Initialize components
	store := NewCompletedCheckpointStore()
	source := &SourceCoordinator{currentOffset: 0}
	sink := NewTwoPhaseCommitSink()
	jm := NewJobManager(store, source, sink)

	// 1. Process first batch of events
	fmt.Println("Processing Batch 1...")
	cp1 := jm.TriggerCheckpoint()
	for i := 1; i <= 3; i++ {
		event := Event{SeqNo: i, Data: fmt.Sprintf("Data-%d", i)}
		sink.Write(event, cp1)
		source.SetOffset(i)
	}
	// Successfully commit Checkpoint 1
	jm.CommitCheckpoint(cp1, source.GetOffset())
	fmt.Printf("Checkpoint %d committed. Source Offset: %d\n", cp1, source.GetOffset())

	// 2. Process second batch of events
	fmt.Println("\nProcessing Batch 2...")
	cp2 := jm.TriggerCheckpoint()
	for i := 4; i <= 6; i++ {
		event := Event{SeqNo: i, Data: fmt.Sprintf("Data-%d", i)}
		sink.Write(event, cp2)
		source.SetOffset(i)
	}
	
	// Simulate JobManager failure BEFORE committing Checkpoint 2 to the HA store.
	fmt.Println("JobManager crashes before committing Checkpoint 2 to HA store!")
	
	// 3. Perform Failover and Recovery
	jm.Failover()

	// 4. Resume processing after recovery
	fmt.Println("\nResuming processing after recovery...")
	cpResume := jm.TriggerCheckpoint()
	
	// Source starts reading from the restored offset (3)
	startOffset := source.GetOffset()
	fmt.Printf("Source resuming from offset: %d\n", startOffset)
	for i := startOffset + 1; i <= startOffset+3; i++ {
		event := Event{SeqNo: i, Data: fmt.Sprintf("Data-%d", i)}
		sink.Write(event, cpResume)
		source.SetOffset(i)
	}
	jm.CommitCheckpoint(cpResume, source.GetOffset())
	fmt.Printf("Checkpoint %d committed. Source Offset: %d\n", cpResume, source.GetOffset())

	// Verify final committed data in the sink
	fmt.Println("\n--- Verification ---")
	fmt.Println("Committed events in Sink:")
	duplicateFound := false
	seen := make(map[int]bool)
	for _, event := range sink.committedData {
		fmt.Printf(" - Event %d: %s\n", event.SeqNo, event.Data)
		if seen[event.SeqNo] {
			duplicateFound = true
		}
		seen[event.SeqNo] = true
	}

	if duplicateFound {
		fmt.Println("FAIL: Duplicate events detected!")
	} else {
		fmt.Println("SUCCESS: Exactly-once processing guaranteed. No duplicates detected.")
	}
}