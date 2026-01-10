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
	Val         float64
	Cnt         int // number of exchanges contributing to the price index
	BidVWAP     float64
	BidQtyTotal float64
	AskVWAP     float64
	AskQtyTotal float64
}

type PriceIndexBuffer = [NUM_SYMBOLS]PriceIndex
type ShmLayout struct {
	Tickers      TickerBuffer
	PriceIndices PriceIndexBuffer
}

// initial weights for each exchange used in the exp mov avg VWAP calculation
var exchangeWeights = [NUM_SYMBOLS][schema.NUM_EXCHANGES]float64{}

func initWeights() {
	for symIx := 0; symIx < NUM_SYMBOLS; symIx++ {
		exchangeWeights[symIx] = [schema.NUM_EXCHANGES]float64{
			0.40,  // BINANCE
			0.075, // BYBIT
			0.072, // COINBASE
			0.074, // GATEIO
			0.068, // HTX
			0.070, // KUCOIN
			0.10,  // MEXC
			0.05,  // OKX
		}
	}
}

/*
UpdatePriceIndex builds a composite price index for a given symbol across all exchanges.

High-level logic:
1. Compute a liquidity-weighted composite mid-price using per-exchange weights.
2. Aggregate bid/ask quantities and notionals to compute side-specific VWAPs.
3. Adapt per-exchange weights slowly (EMA) based on observed liquidity share.
*/
func (s *ShmLayout) UpdatePriceIndex(symIx int) {
	// Accumulates weighted mid-prices across exchanges
	weightedMidPrice := 0.0
	// Sum of weights actually contributing to the index
	totalWeight := 0.0

	// Aggregate quantities and notionals across all exchanges
	bidQtyTotal := 0.0
	askQtyTotal := 0.0
	bidNotTotal := 0.0
	askNotTotal := 0.0

	// Number of exchanges with valid bid/ask data (for diagnostics/metadata)
	numValidExchanges := 0

	// Track total displayed liquidity per exchange for weight adaptation
	qtyPerExchange := [schema.NUM_EXCHANGES]float64{}

	// First pass:
	// - Validate exchange data
	// - Accumulate composite mid-price
	// - Accumulate global bid/ask liquidity and notionals
	for exchIx := 0; exchIx < int(schema.NUM_EXCHANGES); exchIx++ {
		qtyPerExchange[exchIx] = 0.0
		ticker := s.Tickers[exchIx][symIx]

		// Require finite bid/ask prices and quantities
		if !math.IsNaN(ticker.Bid) && !math.IsNaN(ticker.Ask) &&
			!math.IsNaN(ticker.BidQty) && !math.IsNaN(ticker.AskQty) {

			numValidExchanges++

			// Total visible liquidity at top-of-book
			totalQty := ticker.BidQty + ticker.AskQty
			if totalQty > 0 {
				// Store per-exchange liquidity for later weight adaptation
				qtyPerExchange[exchIx] = totalQty

				// Accumulate global bid/ask quantities and notionals
				bidQtyTotal += ticker.BidQty
				askQtyTotal += ticker.AskQty
				bidNotTotal += ticker.BidQty * ticker.Bid
				askNotTotal += ticker.AskQty * ticker.Ask

				// Mid-price used as the exchange price contribution
				midPrice := (ticker.Bid + ticker.Ask) / 2

				// Add weighted mid-price to composite index
				weightedMidPrice += midPrice * exchangeWeights[symIx][exchIx]
				totalWeight += exchangeWeights[symIx][exchIx]
			}
		}
	}

	// Second pass:
	// Adapt per-exchange weights using an EMA of observed liquidity share
	for exchIx := 0; exchIx < int(schema.NUM_EXCHANGES); exchIx++ {
		if qtyPerExchange[exchIx] > 0 {
			// Exchange's fraction of total displayed liquidity
			qtyRatio := qtyPerExchange[exchIx] / (bidQtyTotal + askQtyTotal)

			// Slow adaptation to avoid sudden regime shifts
			exchangeWeights[symIx][exchIx] =
				0.99*exchangeWeights[symIx][exchIx] + 0.01*qtyRatio
		}
	}

	// Finalize index values
	if totalWeight > 0 {
		// Composite liquidity-weighted mid-price
		s.PriceIndices[symIx].Val = weightedMidPrice / totalWeight

		// Metadata and diagnostics
		s.PriceIndices[symIx].Cnt = numValidExchanges
		s.PriceIndices[symIx].AskQtyTotal = askQtyTotal
		s.PriceIndices[symIx].BidQtyTotal = bidQtyTotal

		// Side-specific VWAPs across all exchanges
		s.PriceIndices[symIx].AskVWAP = askNotTotal / askQtyTotal
		s.PriceIndices[symIx].BidVWAP = bidNotTotal / bidQtyTotal
	} else {
		// No usable exchange data
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
	savePeriod := flag.Int("save-period", 100, "Period (ms) to save price indices to the database")
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
	initWeights()
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
			shmData.Tickers[exchange][symIx] = schema.TickerData{
				Bid:    math.NaN(),
				Ask:    math.NaN(),
				BidQty: math.NaN(),
				AskQty: math.NaN(),
			}
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
		saveToDb(normalizedSymbols, shmData, *savePeriod)
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	shouldClose = true
}

func saveToDb(normalizedSymbols []string, shmData *ShmLayout, savePeriod int) {
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

	go func() {
		defer dbWriter.Close()

		tickMs := time.NewTicker(time.Millisecond * time.Duration(savePeriod))
		defer tickMs.Stop()
		for {
			select {
			case <-tickMs.C:
				for symIx := 0; symIx < len(normalizedSymbols); symIx++ {
					priceIdx := shmData.PriceIndices[symIx]
					if !math.IsNaN(priceIdx.Val) {
						symbol := normalizedSymbols[symIx]
						_ = dbWriter.InsertPriceIndex(
							symbol,
							priceIdx.Val,
							priceIdx.Cnt,
							priceIdx.BidVWAP,
							priceIdx.AskVWAP,
							priceIdx.BidQtyTotal,
							priceIdx.AskQtyTotal,
						)
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
