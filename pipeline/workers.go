package pipeline

import (
	"log"
	"time"

	"github.com/xuforr/go-iex/iextp/tops"
)

// messageParsingWorker filters messages and extracts trade data
func (p *Pipeline) messageParsingWorker() {
	defer p.wg.Done()
	
	processed := 0
	filtered := 0
	
	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Message parser: context cancelled, processed %d messages, filtered %d trades", processed, filtered)
			return
		case messageData, ok := <-p.messageChan:
			if !ok {
				log.Printf("Message parser: channel closed, processed %d messages, filtered %d trades", processed, filtered)
				return
			}
			
			processed++
			
			// Filter for trade messages
			if trade, ok := messageData.Message.(*tops.TradeReportMessage); ok {
				select {
				case p.batchChan <- TradeBatch{
					Trades:    []*tops.TradeReportMessage{trade},
					OpenTime:  trade.Timestamp,
					CloseTime: trade.Timestamp,
				}:
					filtered++
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}

// consolidationWorker aggregates trades by time windows
func (p *Pipeline) consolidationWorker() {
	defer p.wg.Done()
	defer close(p.entryChan)
	
	var currentBatch TradeBatch
	var windowStart time.Time
	processed := 0
	
	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Consolidation worker: context cancelled, processed %d batches", processed)
			return
		case batch, ok := <-p.batchChan:
			if !ok {
				// Flush remaining batch
				if len(currentBatch.Trades) > 0 {
					p.flushBatch(&currentBatch)
				}
				log.Printf("Consolidation worker: channel closed, processed %d batches", processed)
				return
			}
			
			trade := batch.Trades[0]
			tradeWindow := trade.Timestamp.Truncate(p.config.TimeWindow)
			
			// If this is a new time window, flush current batch
			if !windowStart.Equal(tradeWindow) && len(currentBatch.Trades) > 0 {
				p.flushBatch(&currentBatch)
				currentBatch = TradeBatch{
					Trades:    []*tops.TradeReportMessage{},
					OpenTime:  tradeWindow,
					CloseTime: tradeWindow.Add(p.config.TimeWindow),
				}
				processed++
			}
			
			// Initialize new window if needed
			if windowStart.IsZero() || !windowStart.Equal(tradeWindow) {
				windowStart = tradeWindow
				currentBatch = TradeBatch{
					Trades:    []*tops.TradeReportMessage{},
					OpenTime:  tradeWindow,
					CloseTime: tradeWindow.Add(p.config.TimeWindow),
				}
			}
			
			// Add trade to current batch
			currentBatch.Trades = append(currentBatch.Trades, trade)
			
			// Flush if batch is large enough
			if len(currentBatch.Trades) >= p.config.BatchSize {
				p.flushBatch(&currentBatch)
				currentBatch.Trades = currentBatch.Trades[:0] // Reset slice but keep capacity
				processed++
			}
		}
	}
}

// batchProcessingWorker converts trade batches to entry batches
func (p *Pipeline) batchProcessingWorker() {
	defer p.wg.Done()
	
	processed := 0
	
	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Batch processor: context cancelled, processed %d entry batches", processed)
			return
		case entryBatch, ok := <-p.entryChan:
			if !ok {
				log.Printf("Batch processor: channel closed, processed %d entry batches", processed)
				return
			}
			
			// Here we would normally send to a writer, but for now just count
			processed++
			
			// In real implementation, would send to writer workers
			_ = entryBatch
		}
	}
}

// flushBatch converts a trade batch to entry batch and sends it downstream
func (p *Pipeline) flushBatch(batch *TradeBatch) {
	if len(batch.Trades) == 0 {
		return
	}
	
	// Import consolidator logic here
	bars := p.makeEntries(batch.Trades, batch.OpenTime, batch.CloseTime)
	
	entryBatch := EntryBatch{
		Entries:   bars,
		Timestamp: batch.OpenTime,
	}
	
	select {
	case p.entryChan <- entryBatch:
		// Success
	case <-p.ctx.Done():
		return
	default:
		log.Printf("Warning: entry channel full, dropping batch")
	}
}

// makeEntries converts trades to consolidated entries (OHLC bars)
func (p *Pipeline) makeEntries(trades []*tops.TradeReportMessage, openTime, closeTime time.Time) []Entry {
	// Group trades by symbol
	bySymbol := make(map[string][]*tops.TradeReportMessage)
	for _, trade := range trades {
		bySymbol[trade.Symbol] = append(bySymbol[trade.Symbol], trade)
	}
	
	var entries []Entry
	for symbol, symbolTrades := range bySymbol {
		entry := p.makeEntry(symbol, symbolTrades, openTime)
		entries = append(entries, entry)
	}
	
	return entries
}

// makeEntry creates a single OHLC entry for a symbol
func (p *Pipeline) makeEntry(symbol string, trades []*tops.TradeReportMessage, openTime time.Time) Entry {
	if len(trades) == 0 {
		return Entry{}
	}
	
	// Sort trades by timestamp (should already be sorted but ensure it)
	// sort.Slice(trades, func(i, j int) bool { return trades[i].Timestamp.Before(trades[j].Timestamp) })
	
	entry := Entry{
		Symbol: symbol,
		Time:   openTime,
		IsTradingHour: isTradingHour(openTime),
	}
	
	for i, trade := range trades {
		price := trade.Price
		
		if i == 0 {
			entry.Open = price
			entry.High = price
			entry.Low = price
		}
		
		if price > entry.High {
			entry.High = price
		}
		if price < entry.Low {
			entry.Low = price
		}
		
		entry.Close = price
		entry.Volume += int64(trade.Size)
	}
	
	return entry
}

// isTradingHour checks if the given time is within trading hours
// Reuses the optimized timezone from pcap2table
func isTradingHour(t time.Time) bool {
	// Convert to Eastern Time
	est := t.In(nyTZ)
	
	// Check if the time is within trading hours
	tradingStart := time.Date(est.Year(), est.Month(), est.Day(), 9, 30, 0, 0, nyTZ)
	tradingEnd := time.Date(est.Year(), est.Month(), est.Day(), 16, 0, 0, 0, nyTZ)
	return est.Equal(tradingStart) || (est.After(tradingStart) && est.Before(tradingEnd))
}

// Load timezone once at startup to avoid repeated file I/O
var nyTZ = func() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	return loc
}()