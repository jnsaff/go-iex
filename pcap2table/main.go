package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuforr/go-iex"
	"github.com/xuforr/go-iex/consolidator"
	"github.com/xuforr/go-iex/db"
	"github.com/xuforr/go-iex/iextp/tops"
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

func main() {
	config := parseArgs()

	// Use the config values
	fmt.Printf("Pcap Filename: %s\n", config.PcapFilename)
	fmt.Printf("MySQL Config File: %s\n", config.MySQLConfigFile)
	fmt.Printf("Status Report Gap: %d\n", config.StatusReportGap)
	fmt.Printf("Also Write To CSV: %s\n", config.CsvFile)

	processPcapFile(config)
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
