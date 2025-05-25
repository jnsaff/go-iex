package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuforr/go-iex"
	"github.com/xuforr/go-iex/consolidator"
	"github.com/xuforr/go-iex/db"
	"github.com/xuforr/go-iex/iextp/tops"
	"github.com/xuforr/go-iex/pipeline"
)

var header = []string{
	"symbol",
	"time",
	"open",
	"high",
	"low",
	"close",
	"volume",
	"istradinghour",
}

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

type Writer interface {
	Write([]string) error
}

type Config struct {
	PcapFilename    string
	MySQLConfigFile string
	StatusReportGap int
	CsvFile         string
}

// PipelineMode contains configuration for pipeline processing
type PipelineMode struct {
	PcapFilename    string
	MySQLConfigFile string
	StatusReportGap int
	CsvFile         string
	UsePipeline     bool
	NumWorkers      int
}

func main() {
	// Check if pipeline flag is present
	usePipeline := flag.Bool("pipeline", false, "Use pipelined processing for better performance")
	numWorkers := flag.Int("workers", 4, "Number of worker goroutines for pipeline mode")
	
	if len(os.Args) > 1 && contains(os.Args, "-pipeline") {
		mainWithPipeline()
		return
	}
	
	config := parseArgs()

	// Use the config values
	fmt.Printf("Pcap Filename: %s\n", config.PcapFilename)
	fmt.Printf("MySQL Config File: %s\n", config.MySQLConfigFile)
	fmt.Printf("Status Report Gap: %d\n", config.StatusReportGap)
	fmt.Printf("Also Write To CSV: %s\n", config.CsvFile)
	
	if *usePipeline {
		fmt.Printf("Number of Workers: %d\n", *numWorkers)
		fmt.Println("Using pipelined processing for enhanced performance")
		
		pipelineConfig := PipelineMode{
			PcapFilename:    config.PcapFilename,
			MySQLConfigFile: config.MySQLConfigFile,
			StatusReportGap: config.StatusReportGap,
			CsvFile:         config.CsvFile,
			UsePipeline:     true,
			NumWorkers:      *numWorkers,
		}
		
		if err := runPipelineMode(pipelineConfig); err != nil {
			log.Fatalf("Pipeline processing failed: %v", err)
		}
	} else {
		fmt.Println("Using original sequential processing")
		processPcapFile(config)
	}
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func parseArgs() Config {
	pcapFilename := flag.String("pcap", "", "Path to the pcap file")
	mySQLConfigFile := flag.String("db", "", "Path to the MySQL config file")
	csvFile := flag.String("csv", "", "Path to the CSV file")
	statusReportGap := flag.Int("status_print_interval", 0, "Status report interval")

	flag.Parse()

	if *pcapFilename == "" || (*mySQLConfigFile == "" && *csvFile == "") {
		fmt.Println("Please provide the required arguments")
		flag.Usage()
		os.Exit(1)
	}

	return Config{
		PcapFilename:    *pcapFilename,
		MySQLConfigFile: *mySQLConfigFile,
		StatusReportGap: *statusReportGap,
		CsvFile:         *csvFile,
	}
}

type CombinedWriter struct {
	writers []Writer
}

func (w *CombinedWriter) Write(data []string) error {
	for _, writer := range w.writers {
		if err := writer.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func processPcapFile(config Config) {
	// Open the pcap file
	pcapFile, err := os.Open(config.PcapFilename)
	if err != nil {
		log.Fatalf("Failed to open pcap file: %v", err)
	}
	defer pcapFile.Close()

	// Create a CombinedWriter to write to MySQL and optionally to CSV
	writers := &CombinedWriter{
		writers: []Writer{},
	}

	// Connect to MySQL
	if config.MySQLConfigFile != "" {
		db, err := db.NewDB(config.MySQLConfigFile)
		if err != nil {
			log.Fatalf("Failed to connect to MySQL: %v", err)
		}
		defer db.Close()
		writers.writers = append(writers.writers, db)
		fmt.Println("Successfully connected to MySQL!")
	}

	var csvWriter *csv.Writer
	if config.CsvFile != "" {
		var csvFile *os.File
		csvFile, err = os.Create(config.CsvFile)
		if err != nil {
			log.Fatal(err)
		}
		defer csvFile.Close()
		
		// Use buffered writer for better CSV write performance
		bufferedCSV := bufio.NewWriterSize(csvFile, 256*1024)
		defer bufferedCSV.Flush()
		csvWriter = csv.NewWriter(bufferedCSV)
		if err := csvWriter.Write(header); err != nil {
			log.Fatal(err)
		}
		defer csvWriter.Flush()
		writers.writers = append(writers.writers, csvWriter)
		fmt.Println("Successfully created CSV file!")
	}

	// Process the pcap file and write to MySQL and optionally to CSV
	processAndWrite(pcapFile, writers, config.StatusReportGap)
}

func computeOpenAndCloseTime(t time.Time) (time.Time, time.Time) {
	openTime := t.Truncate(time.Minute)
	closeTime := openTime.Add(time.Minute)
	return openTime, closeTime
}

func processAndWrite(pcapFile *os.File, w Writer, statusReportGap int) {
	// Create a packet source and scanner to read the pcap file
	packetSource, err := iex.NewPacketDataSource(pcapFile)
	scanner := iex.NewPcapScanner(packetSource)
	if err != nil {
		log.Fatal(err)
	}

	var trades []*tops.TradeReportMessage
	var openTime, closeTime time.Time
	parsed := 0
	done := false

	for !done {
		msg, err := scanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				done = true
				continue
			}
			log.Fatal(err)
		}

		if msg, ok := msg.(*tops.TradeReportMessage); ok {
			if openTime.IsZero() {
				openTime, closeTime = computeOpenAndCloseTime(msg.Timestamp)
			}

			// All trades for this unit has been accumulated
			if msg.Timestamp.After(closeTime) && len(trades) > 0 {
				entries := makeEntries(trades, openTime, closeTime)
				if err := writeEntries(entries, w); err != nil {
					log.Fatal(err)
				}

				trades = trades[:0]
				openTime, closeTime = computeOpenAndCloseTime(msg.Timestamp)
			}

			trades = append(trades, msg)
			parsed = parsed + 1
			if statusReportGap > 0 && parsed%statusReportGap == 0 {
				fmt.Printf("Processed %d records\n", parsed)
			}
		}
	}

}

func makeEntries(trades []*tops.TradeReportMessage, openTime, closeTime time.Time) map[string]Entry {
	bars := consolidator.MakeBars(trades)
	for _, bar := range bars {
		bar.OpenTime = openTime
		bar.CloseTime = closeTime
	}

	entries := make(map[string]Entry)
	for _, bar := range bars {
		entry := Entry{
			Symbol:        bar.Symbol,
			Time:          bar.OpenTime,
			Open:          bar.Open,
			High:          bar.High,
			Low:           bar.Low,
			Close:         bar.Close,
			Volume:        bar.Volume,
			IsTradingHour: isTradingHour(bar.OpenTime),
		}
		entries[bar.Symbol] = entry
	}

	return entries
}

func writeSingleEntry(entry *Entry, w Writer) error {
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

	return w.Write(row)
}

// Load timezone once at startup to avoid repeated file I/O
var nyTZ = func() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatal(err)
	}
	return loc
}()

func isTradingHour(t time.Time) bool {
	// Convert the time to Eastern Time (handles both EST and EDT)
	est := t.In(nyTZ)

	// Check if the time is within trading hours
	tradingStart := time.Date(est.Year(), est.Month(), est.Day(), 9, 30, 0, 0, nyTZ)
	tradingEnd := time.Date(est.Year(), est.Month(), est.Day(), 16, 0, 0, 0, nyTZ)
	return est.Equal(tradingStart) || (est.After(tradingStart) && est.Before(tradingEnd))
}

func writeEntries(entries map[string]Entry, w Writer) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		entry := entries[key]
		if err := writeSingleEntry(&entry, w); err != nil {
			return err
		}
	}

	return nil
}

// Pipeline processing functions

func runPipelineMode(config PipelineMode) error {
	// Create pipeline configuration
	pipelineConfig := pipeline.DefaultPipelineConfig()
	pipelineConfig.NumParseWorkers = config.NumWorkers / 2
	if pipelineConfig.NumParseWorkers < 1 {
		pipelineConfig.NumParseWorkers = 1
	}
	pipelineConfig.NumBatchWorkers = config.NumWorkers / 2
	if pipelineConfig.NumBatchWorkers < 1 {
		pipelineConfig.NumBatchWorkers = 1
	}

	fmt.Printf("Starting pipeline processing with %d parse workers and %d batch workers\n", 
		pipelineConfig.NumParseWorkers, pipelineConfig.NumBatchWorkers)

	// Create pipeline
	p := pipeline.NewPipeline(pipelineConfig)

	// Create writers
	var writers []pipeline.Writer

	// Add MySQL writer if configured
	if config.MySQLConfigFile != "" {
		dbWriter, err := db.NewDB(config.MySQLConfigFile)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %v", err)
		}
		defer dbWriter.Close()
		writers = append(writers, dbWriter)
		fmt.Println("Successfully connected to MySQL!")
	}

	// Add CSV writer if configured
	if config.CsvFile != "" {
		csvFile, err := os.Create(config.CsvFile)
		if err != nil {
			return err
		}
		defer csvFile.Close()

		// Use buffered writer for better CSV write performance
		bufferedCSV := bufio.NewWriterSize(csvFile, 256*1024)
		defer bufferedCSV.Flush()
		csvWriter := csv.NewWriter(bufferedCSV)
		defer csvWriter.Flush()

		// Write header
		if err := csvWriter.Write(header); err != nil {
			return err
		}

		writers = append(writers, csvWriter)
		fmt.Println("Successfully created CSV file!")
	}

	// Create entry writer
	entryWriter := pipeline.NewEntryWriter(writers)

	// Start pipeline
	p.Start()

	// Create writer workers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writerWG sync.WaitGroup
	numWriterWorkers := config.NumWorkers / 4
	if numWriterWorkers < 1 {
		numWriterWorkers = 1
	}

	for i := 0; i < numWriterWorkers; i++ {
		writerWG.Add(1)
		go pipeline.WriterWorker(p.EntryChannel(), entryWriter, ctx, &writerWG)
	}

	// Create and start packet reader
	reader, err := pipeline.NewPacketReader(
		config.PcapFilename, 
		p.MessageChannel(), 
		config.StatusReportGap, 
		ctx,
	)
	if err != nil {
		return fmt.Errorf("failed to create packet reader: %v", err)
	}

	fmt.Printf("Processing pcap file: %s\n", config.PcapFilename)
	startTime := time.Now()

	// Run packet reader (this blocks until complete)
	if err := reader.Run(); err != nil {
		return fmt.Errorf("packet reader error: %v", err)
	}

	fmt.Println("Packet reading complete, shutting down pipeline...")

	// Shutdown pipeline
	p.Stop()

	// Cancel writer context and wait for completion
	cancel()
	writerWG.Wait()

	duration := time.Since(startTime)
	fmt.Printf("Processing completed in %v\n", duration)

	return nil
}

func mainWithPipeline() {
	pcapFilename := flag.String("pcap", "", "Path to the pcap file")
	mySQLConfigFile := flag.String("db", "", "Path to the MySQL config file")
	csvFile := flag.String("csv", "", "Path to the CSV file")
	statusReportGap := flag.Int("status_print_interval", 0, "Status report interval")
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines")

	flag.Parse()

	if *pcapFilename == "" || (*mySQLConfigFile == "" && *csvFile == "") {
		fmt.Println("Please provide the required arguments")
		flag.Usage()
		os.Exit(1)
	}

	config := PipelineMode{
		PcapFilename:    *pcapFilename,
		MySQLConfigFile: *mySQLConfigFile,
		StatusReportGap: *statusReportGap,
		CsvFile:         *csvFile,
		UsePipeline:     true,
		NumWorkers:      *numWorkers,
	}

	fmt.Println("Using pipelined processing for enhanced performance")
	if err := runPipelineMode(config); err != nil {
		log.Fatalf("Pipeline processing failed: %v", err)
	}
}
