package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/xuforr/go-iex/iextp/tops"
)

// PipelineConfig contains configuration for the processing pipeline
type PipelineConfig struct {
	MessageBufferSize   int           // Size of parsed message channel buffer
	BatchBufferSize     int           // Size of batch channel buffer
	EntryBufferSize     int           // Size of entry channel buffer
	TimeWindow          time.Duration // Time window for trade aggregation
	BatchSize           int           // Number of trades per batch processing
	NumParseWorkers     int           // Number of message parsing workers
	NumBatchWorkers     int           // Number of batch processing workers
}

// DefaultPipelineConfig returns optimized defaults for modern hardware
func DefaultPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		MessageBufferSize: 50000,  // Large buffer for parsed messages
		BatchBufferSize:   5000,   // Large buffer for trade batches
		EntryBufferSize:   1000,   // Moderate buffer for entry batches
		TimeWindow:        time.Minute,
		BatchSize:         10000,  // Trades per batch
		NumParseWorkers:   2,      // Message parsing workers
		NumBatchWorkers:   2,      // Batch processing workers
	}
}

// TradeMessage represents a parsed trade with metadata
type TradeMessage struct {
	Trade     *tops.TradeReportMessage
	Timestamp time.Time
}

// TradeBatch represents aggregated trades for a time window
type TradeBatch struct {
	Trades    []*tops.TradeReportMessage
	OpenTime  time.Time
	CloseTime time.Time
}

// EntryBatch represents processed entries ready for writing
type EntryBatch struct {
	Entries   []Entry
	Timestamp time.Time
}

// Entry represents a consolidated OHLC bar with trading hour info
type Entry struct {
	Symbol        string
	Time          time.Time
	Open          float64
	High          float64
	Low           float64
	Close         float64
	Volume        int64
	IsTradingHour bool
}

// Writer interface for output destinations
type Writer interface {
	Write([]string) error
}

// EntryWriter handles writing entry batches to output destinations
type EntryWriter struct {
	writers []Writer
}

// Pipeline coordinates the multi-stage processing pipeline
type Pipeline struct {
	config *PipelineConfig
	
	// Channels for pipeline stages
	messageChan chan MessageData
	batchChan   chan TradeBatch
	entryChan   chan EntryBatch
	
	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
	
	// WaitGroup for coordinating workers
	wg sync.WaitGroup
}

// NewPipeline creates a new processing pipeline
func NewPipeline(config *PipelineConfig) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Pipeline{
		config:      config,
		messageChan: make(chan MessageData, config.MessageBufferSize),
		batchChan:   make(chan TradeBatch, config.BatchBufferSize),
		entryChan:   make(chan EntryBatch, config.EntryBufferSize),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start initializes all pipeline stages
func (p *Pipeline) Start() {
	// Start message parsing workers
	for i := 0; i < p.config.NumParseWorkers; i++ {
		p.wg.Add(1)
		go p.messageParsingWorker()
	}
	
	// Start consolidation worker (single worker for time-ordered processing)
	p.wg.Add(1)
	go p.consolidationWorker()
	
	// Start batch processing workers
	for i := 0; i < p.config.NumBatchWorkers; i++ {
		p.wg.Add(1)
		go p.batchProcessingWorker()
	}
}

// Stop gracefully shuts down the pipeline
func (p *Pipeline) Stop() {
	p.cancel()
	close(p.messageChan)
	p.wg.Wait()
}

// MessageChannel returns the channel for sending messages
func (p *Pipeline) MessageChannel() chan<- MessageData {
	return p.messageChan
}

// EntryChannel returns the channel for receiving processed entries
func (p *Pipeline) EntryChannel() <-chan EntryBatch {
	return p.entryChan
}