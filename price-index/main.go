package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"time"

	"price-index/schema"
	"price-index/ws"
)

// NUM_SYMBOLS defines the preallocated number of symbols per exchange.
const NUM_SYMBOLS = 128

type TickerBuffer = [schema.NUM_EXCHANGES][NUM_SYMBOLS]schema.TickerData

type PriceIndex struct {
	Val float64
	Cnt int // number of exchanges contributing to the price index
}

type PriceIndexBuffer = [NUM_SYMBOLS]PriceIndex
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
		s.PriceIndices[symIx].Val = (bidSum + askSum) / (2.0 * count)
		s.PriceIndices[symIx].Cnt = int(count)
	} else {
		s.PriceIndices[symIx].Val = math.NaN()
	}
}

type ExchangeInfo struct {
	Symbols [][]string `json:"symbols"`
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	shouldClose := false

	saveDb := flag.Bool("save-db", true, "Whether to save price indices to the database")
	exchangeInfoPath := flag.String("exchange-info", "exchange_info.json", "Path to exchange info JSON file")
	savePeriod := flag.Int("save-period", 10, "Period (ms) to save price indices to the database")
	flag.Parse()

	var shmData *ShmLayout

	shmPath := os.Getenv("SHM_PATH")
	if shmPath == "" {
		shmPath = ".price_ix.data"
	}
	w, err := NewSHMWriter[ShmLayout](shmPath)
	defer w.Close()
	if err != nil {
		panic(err)
	}
	shmData = w.Data
	data, err := os.ReadFile(*exchangeInfoPath)
	if err != nil {
		panic(err)
	}

	var exchangeInfo ExchangeInfo
	if err := json.Unmarshal(data, &exchangeInfo); err != nil {
		panic(err)
	}
	normalizedSymbols := exchangeInfo.Symbols[0]
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
			shmData.PriceIndices[symIx].Val = math.NaN()
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
		}
	}()
	if *saveDb {
		// Connect to database
		// Connect to database
		newFunction(normalizedSymbols, shmData, *savePeriod)
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	shouldClose = true
}

func newFunction(normalizedSymbols []string, shmData *ShmLayout, savePeriod int) {
	var dbWriter *DatabaseWriter

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

	go func() {
		tick10ms := time.NewTicker(time.Millisecond * time.Duration(savePeriod))
		defer tick10ms.Stop()
		for {
			select {
			case <-tick10ms.C:
				for symIx := 0; symIx < len(normalizedSymbols); symIx++ {
					price := shmData.PriceIndices[symIx].Val
					numEx := shmData.PriceIndices[symIx].Cnt
					if !math.IsNaN(price) {
						symbol := normalizedSymbols[symIx]
						_ = dbWriter.InsertPriceIndex(symbol, price, numEx)
					}
				}
			}
		}
	}()
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
