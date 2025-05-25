package pipeline

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)

// NewEntryWriter creates a new entry writer with multiple output destinations
func NewEntryWriter(writers []Writer) *EntryWriter {
	return &EntryWriter{
		writers: writers,
	}
}

// WriteEntry converts an entry to string slice and writes to all destinations
func (ew *EntryWriter) WriteEntry(entry Entry) error {
	row := []string{
		entry.Symbol,
		entry.Time.Format(time.RFC3339),
		strconv.FormatFloat(entry.Open, 'f', 4, 64),
		strconv.FormatFloat(entry.High, 'f', 4, 64),
		strconv.FormatFloat(entry.Low, 'f', 4, 64),
		strconv.FormatFloat(entry.Close, 'f', 4, 64),
		strconv.FormatInt(entry.Volume, 10),
		strconv.FormatBool(entry.IsTradingHour),
	}
	
	for _, writer := range ew.writers {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	
	return nil
}

// WriteBatch writes a batch of entries
func (ew *EntryWriter) WriteBatch(batch EntryBatch) error {
	for _, entry := range batch.Entries {
		if err := ew.WriteEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

// BatchWriter handles batched writing with configurable batch sizes
type BatchWriter struct {
	entryWriter *EntryWriter
	batchSize   int
	buffer      []Entry
	mu          sync.Mutex
	ctx         context.Context
}

// NewBatchWriter creates a new batch writer
func NewBatchWriter(entryWriter *EntryWriter, batchSize int, ctx context.Context) *BatchWriter {
	return &BatchWriter{
		entryWriter: entryWriter,
		batchSize:   batchSize,
		buffer:      make([]Entry, 0, batchSize),
		ctx:         ctx,
	}
}

// Add adds an entry to the batch buffer
func (bw *BatchWriter) Add(entry Entry) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	
	bw.buffer = append(bw.buffer, entry)
	
	if len(bw.buffer) >= bw.batchSize {
		return bw.flushLocked()
	}
	
	return nil
}

// Flush writes all buffered entries
func (bw *BatchWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	
	return bw.flushLocked()
}

// flushLocked flushes the buffer without acquiring the lock
func (bw *BatchWriter) flushLocked() error {
	if len(bw.buffer) == 0 {
		return nil
	}
	
	batch := EntryBatch{
		Entries:   make([]Entry, len(bw.buffer)),
		Timestamp: time.Now(),
	}
	copy(batch.Entries, bw.buffer)
	
	err := bw.entryWriter.WriteBatch(batch)
	bw.buffer = bw.buffer[:0] // Reset slice but keep capacity
	
	return err
}

// WriterWorker processes entry batches from a channel
func WriterWorker(entryChan <-chan EntryBatch, entryWriter *EntryWriter, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	
	processed := 0
	entries := 0
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("Writer worker: context cancelled, processed %d batches (%d entries)", processed, entries)
			return
		case batch, ok := <-entryChan:
			if !ok {
				log.Printf("Writer worker: channel closed, processed %d batches (%d entries)", processed, entries)
				return
			}
			
			if err := entryWriter.WriteBatch(batch); err != nil {
				log.Printf("Writer worker: error writing batch: %v", err)
				continue
			}
			
			processed++
			entries += len(batch.Entries)
			
			if processed%100 == 0 {
				log.Printf("Writer worker: processed %d batches (%d entries)", processed, entries)
			}
		}
	}
}

// MultiWriter combines multiple writers for parallel output
type MultiWriter struct {
	writers []Writer
}

// NewMultiWriter creates a writer that writes to multiple destinations
func NewMultiWriter(writers ...Writer) *MultiWriter {
	return &MultiWriter{
		writers: writers,
	}
}

// Write writes to all destinations
func (mw *MultiWriter) Write(data []string) error {
	for _, writer := range mw.writers {
		if err := writer.Write(data); err != nil {
			return err
		}
	}
	return nil
}