package schema

import (
	"price-index/pb"
	"strconv"
)

type Exchange int

const (
	Binance Exchange = iota
	Bybit
	Coinbase
	Gateio
	HTX
	Kucoin
	Mexc
	OKX
	NUM_EXCHANGES
)

func (e Exchange) String() string {
	if e < 0 || e >= NUM_EXCHANGES {
		return "Unknown"
	}
	return [...]string{"Binance", "Bybit", "Coinbase", "Gateio", "HTX", "Kucoin", "Mexc", "OKX"}[e]
}

type TickerData struct {
	Bid      float64
	Ask      float64
}

type BinanceTicker struct {
	U    int64  `json:"u"`
	S    string `json:"s"`
	B    string `json:"b"`
	BQty string `json:"B"`
	A    string `json:"a"`
	AQty string `json:"A"`
}
type BybitTicker struct {
	Topic string `json:"topic"`
	Ts    int64  `json:"ts"`
	Type  string `json:"type"`
	Data  struct {
		S   string     `json:"s"`
		B   [][]string `json:"b"`
		A   [][]string `json:"a"`
		U   int64      `json:"u"`
		Seq int64      `json:"seq"`
	} `json:"data"`
	Cts int64 `json:"cts"`
}
type CoinbaseTicker struct {
	Type        string `json:"type"`
	Sequence    int64  `json:"sequence"`
	ProductID   string `json:"product_id"`
	Price       string `json:"price"`
	Open24h     string `json:"open_24h"`
	Volume24h   string `json:"volume_24h"`
	Low24h      string `json:"low_24h"`
	High24h     string `json:"high_24h"`
	BestBid     string `json:"best_bid"`
	BestBidSize string `json:"best_bid_size"`
	BestAsk     string `json:"best_ask"`
	BestAskSize string `json:"best_ask_size"`
	Side        string `json:"side"`
	Time        string `json:"time"`
	TradeID     int64  `json:"trade_id"`
	LastSize    string `json:"last_size"`
}
type GateioTicker struct {
	Time    int64  `json:"time"`
	TimeMs  int64  `json:"time_ms"`
	Channel string `json:"channel"`
	Event   string `json:"event"`
	Result  struct {
		T  int64  `json:"t"`
		U  int64  `json:"u"`
		S  string `json:"s"`
		B  string `json:"b"`
		BQ string `json:"B"`
		A  string `json:"a"`
		AQ string `json:"A"`
	} `json:"result"`
}
type HTXTicker struct {
	Ch   string `json:"ch"`
	Ts   int64  `json:"ts"`
	Tick struct {
		SeqId     int64   `json:"seqId"`
		Ask       float64 `json:"ask"`
		AskSize   float64 `json:"askSize"`
		Bid       float64 `json:"bid"`
		BidSize   float64 `json:"bidSize"`
		QuoteTime int64   `json:"quoteTime"`
		Symbol    string  `json:"symbol"`
	} `json:"tick"`
}
type KucoinTicker struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Subject string `json:"subject"`
	Data    struct {
		BestAsk     string `json:"bestAsk"`
		BestAskSize string `json:"bestAskSize"`
		BestBid     string `json:"bestBid"`
		BestBidSize string `json:"bestBidSize"`
		Price       string `json:"price"`
		Sequence    string `json:"sequence"`
		Size        string `json:"size"`
		Time        int64  `json:"time"`
	} `json:"data"`
}
type OKXTicker struct {
	Arg struct {
		Channel string `json:"channel"`
		InstId  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		Asks  [][]string `json:"asks"`
		Bids  [][]string `json:"bids"`
		Ts    string     `json:"ts"`
		SeqId int64      `json:"seqId"`
	} `json:"data"`
}
type MexcTicker struct {
	pb.BookTicker
}

func (t *TickerData) Parse(ticker any) {
	switch v := ticker.(type) {
	case *BinanceTicker:
		bid, _ := strconv.ParseFloat(v.B, 64)
		ask, _ := strconv.ParseFloat(v.A, 64)
		t.Bid = bid
		t.Ask = ask
	case *BybitTicker:
		bid, _ := strconv.ParseFloat(v.Data.B[0][0], 64)
		ask, _ := strconv.ParseFloat(v.Data.A[0][0], 64)
		t.Bid = bid
		t.Ask = ask
	case *CoinbaseTicker:
		bid, _ := strconv.ParseFloat(v.BestBid, 64)
		ask, _ := strconv.ParseFloat(v.BestAsk, 64)
		t.Bid = bid
		t.Ask = ask
	case *GateioTicker:
		bid, _ := strconv.ParseFloat(v.Result.B, 64)
		ask, _ := strconv.ParseFloat(v.Result.A, 64)
		t.Bid = bid
		t.Ask = ask
	case *HTXTicker:
		t.Bid = v.Tick.Bid
		t.Ask = v.Tick.Ask
	case *KucoinTicker:
		bid, _ := strconv.ParseFloat(v.Data.BestBid, 64)
		ask, _ := strconv.ParseFloat(v.Data.BestAsk, 64)
		t.Bid = bid
		t.Ask = ask
	case *MexcTicker:
		bid, _ := strconv.ParseFloat(v.Publicbookticker.BidPrice, 64)
		ask, _ := strconv.ParseFloat(v.Publicbookticker.AskPrice, 64)
		t.Bid = bid
		t.Ask = ask
	case *OKXTicker:
		bid, _ := strconv.ParseFloat(v.Data[0].Bids[0][0], 64)
		ask, _ := strconv.ParseFloat(v.Data[0].Asks[0][0], 64)
		t.Bid = bid
		t.Ask = ask
	default:
		// Handle unknown ticker type if necessary
	}
}
