package pipeline

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/xuforr/go-iex"
	"github.com/xuforr/go-iex/iextp"
)

// PacketReader handles the packet reading stage of the pipeline
type PacketReader struct {
	file       *os.File
	scanner    *iex.PcapScanner
	messageChan chan<- MessageData
	statusGap  int
	ctx        context.Context
}

// MessageData represents a parsed message from the pcap stream
type MessageData struct {
	Message iextp.Message
}

// NewPacketReader creates a new packet reader
func NewPacketReader(filename string, messageChan chan<- MessageData, statusGap int, ctx context.Context) (*PacketReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	
	packetSource, err := iex.NewPacketDataSource(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	
	scanner := iex.NewPcapScanner(packetSource)
	
	return &PacketReader{
		file:        file,
		scanner:     scanner,
		messageChan: messageChan,
		statusGap:   statusGap,
		ctx:         ctx,
	}, nil
}

// Run starts the packet reading process
func (pr *PacketReader) Run() error {
	defer pr.file.Close()
	defer close(pr.messageChan)
	
	processed := 0
	for {
		select {
		case <-pr.ctx.Done():
			log.Printf("Packet reader: context cancelled, processed %d messages", processed)
			return pr.ctx.Err()
		default:
		}
		
		msg, err := pr.scanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				log.Printf("Packet reader: processed %d messages total", processed)
				break
			}
			return err
		}
		
		// Create message data
		messageData := MessageData{
			Message: msg,
		}
		
		// Send to pipeline with context awareness
		select {
		case pr.messageChan <- messageData:
			processed++
			if pr.statusGap > 0 && processed%pr.statusGap == 0 {
				log.Printf("Packet reader: processed %d messages", processed)
			}
		case <-pr.ctx.Done():
			return pr.ctx.Err()
		}
	}
	
	return nil
}