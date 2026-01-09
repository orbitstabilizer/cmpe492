package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"

	"price-index/schema"
	"price-index/ws"
)

// NUM_SYMBOLS defines the preallocated number of symbols per exchange.
const NUM_SYMBOLS = 128

type TickerBuffer = [schema.NUM_EXCHANGES][NUM_SYMBOLS]schema.TickerData
type PriceIndexBuffer = [NUM_SYMBOLS]float64
type ShmLayout struct {
	Tickers      TickerBuffer
	PriceIndices PriceIndexBuffer
}

/*
UpdatePriceIndex recalculates the price index for a given symbol index across all exchanges.
It computes the average of the mid-prices (average of bid and ask) from all exchanges
that have valid bid and ask prices for the specified symbol.
If no valid prices are found, the price index is set to NaN.
*/
func (s *ShmLayout) UpdatePriceIndex(symIx int) {
	bidSum := 0.0
	askSum := 0.0
	count := 0.0
	for exchIx := 0; exchIx < int(schema.NUM_EXCHANGES); exchIx++ {
		ticker := s.Tickers[exchIx][symIx]
		if !math.IsNaN(ticker.Bid) && !math.IsNaN(ticker.Ask) {
			midPrice := (ticker.Bid + ticker.Ask) / 2.0
			bidSum += midPrice
			askSum += midPrice
			count += 1.0
		}
	}
	if count > 0 {
		s.PriceIndices[symIx] = (bidSum + askSum) / (2.0 * count)
	} else {
		s.PriceIndices[symIx] = math.NaN()
	}
}

type ExchangeInfo struct {
	Symbols [][]string `json:"symbols"`
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	shouldClose := false

	var shmData *ShmLayout
	var dbWriter *DatabaseWriter

	// Connect to database
	// Connect to database
	connStr := os.Getenv("DB_CONN_STR")
	if connStr == "" {
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			panic("Required environment variable missing: POSTGRES_USER")
		}
		password := os.Getenv("POSTGRES_PASSWORD")
		if password == "" {
			panic("Required environment variable missing: POSTGRES_PASSWORD")
		}
		dbname := os.Getenv("POSTGRES_DB")
		if dbname == "" {
			panic("Required environment variable missing: POSTGRES_DB")
		}
		host := os.Getenv("POSTGRES_HOST")
		if host == "" {
			panic("Required environment variable missing: POSTGRES_HOST")
		}
		port := os.Getenv("POSTGRES_PORT")
		if port == "" {
			panic("Required environment variable missing: POSTGRES_PORT")
		}
		connStr = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	}
	var err error
	dbWriter, err = NewDatabaseWriter(connStr)
	if err != nil {
		panic(err)
	}
	defer dbWriter.Close()

	w, err := NewSHMWriter[ShmLayout](".price_ix.data")
	defer w.Close()
	if err != nil {
		panic(err)
	}
	shmData = w.Data
	data, err := os.ReadFile("exchange_info.json")
	if err != nil {
		panic(err)
	}

	var exchangeInfo ExchangeInfo
	if err := json.Unmarshal(data, &exchangeInfo); err != nil {
		panic(err)
	}
	updateChan := make(chan int, 1)
	for i := range exchangeInfo.Symbols {
		if i >= int(schema.NUM_EXCHANGES) {
			panic("NUM_EXCHANGES too small for exchange symbols")
		}
		exchange := schema.Exchange(i)
		for symIx := range exchangeInfo.Symbols[i] {
			if symIx >= NUM_SYMBOLS {
				panic("NUM_SYMBOLS too small for exchange symbols")
			}
			shmData.Tickers[exchange][symIx] = schema.TickerData{Bid: math.NaN(), Ask: math.NaN()}
			shmData.PriceIndices[symIx] = math.NaN()
		}
		go ws.ConnectExchange(
			exchange,
			exchangeInfo.Symbols[exchange],
			shmData.Tickers[exchange][0:len(exchangeInfo.Symbols[exchange])],
			&shouldClose,
			updateChan,
		)
	}
	go func() {
		for symIx := range updateChan {
			shmData.UpdatePriceIndex(symIx)
			price := shmData.PriceIndices[symIx]
			if !math.IsNaN(price) {
				symbol := getSymbolName(symIx, exchangeInfo)
				_ = dbWriter.InsertPriceIndex(symbol, price, getNumValidExchanges(shmData, symIx), 0.0)
			}
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	shouldClose = true
}

func getSymbolName(symIx int, info ExchangeInfo) string {
	// Simple mapping assuming all exchanges have same symbol at same index
	// In reality, we should probably have a unified symbol list
	// But taking from first exchange for now
	if len(info.Symbols) > 0 && symIx < len(info.Symbols[0]) {
		return info.Symbols[0][symIx]
	}
	// fallback
	return "UNKNOWN"
}

func getNumValidExchanges(shmData *ShmLayout, symIx int) int {
	count := 0
	for exchIx := 0; exchIx < int(schema.NUM_EXCHANGES); exchIx++ {
		ticker := shmData.Tickers[exchIx][symIx]
		if !math.IsNaN(ticker.Bid) && !math.IsNaN(ticker.Ask) {
			count++
		}
	}
	return count
}
