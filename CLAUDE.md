# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

**Testing:**
- `go test` - Run tests in current package
- `go test ./...` - Run all tests recursively
- `go test -v ./...` - Run all tests with verbose output
- `go test -short ./...` - Skip long-running pcap tests

**Building utilities:**
- `go install ./pcap2table` - Build OHLC bar generator with trading hours
- `go install ./pcap2csv` - Build pcap to CSV converter
- `go install ./pcap2json` - Build pcap to JSON converter

**Running utilities:**
- `pcap2table -pcaf=<file> -db=<mysql_config> -csv=<output.csv>` - Generate OHLC data with trading hours
- `pcap2csv < input.pcap > output.csv` - Convert pcap to CSV format
- `pcap2json < input.pcap > output.json` - Convert pcap to JSON format

## Architecture Overview

**Core Components:**
- **HTTP API Client** (`client.go`) - IEX Developer API wrapper with `Client` struct
- **IEX-TP Parser** (`iextp/`) - Binary protocol parser for real-time market data
- **Data Consolidation** (`consolidator/`) - OHLC bar generation from tick data
- **Database Integration** (`db/`) - MySQL support for storing market data

**Key Design Patterns:**
- **Interface-based HTTP client** - `HTTPClient` interface enables mocking for tests
- **Protocol message registration** - Message types register themselves in IEX-TP parser
- **Streaming scanner pattern** - `PcapScanner` provides iterator interface for pcap data
- **Type-safe JSON handling** - Custom unmarshaling for financial data with proper null handling

**Data Flow:**
1. **Live data**: Multicast UDP → `PcapScanner` → message types → consolidation
2. **Historical data**: HTTP API → `Client` methods → pcap download → parsing
3. **Export**: Raw data → consolidator → CSV/JSON/MySQL output

**Package Structure:**
- `/` - HTTP API client and main interfaces
- `/iextp/` - IEX Transport Protocol with `/deep/` and `/tops/` sub-packages
- `/consolidator/` - OHLC bar consolidation logic
- `/db/` - Database persistence layer
- `/examples/` - Reference implementations

**Testing Approach:**
- Mock HTTP client with fixture data in `/testdata/responses/`
- Sample pcap files in `/testdata/` for integration tests
- Use `-short` flag to skip time-intensive pcap processing tests
- Comprehensive coverage of JSON unmarshaling edge cases