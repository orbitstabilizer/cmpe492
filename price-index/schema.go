package main

import (
	"fmt"
	"strconv"
	"strings"

	"exchange/pb"
)

type SubscribeBinanceTicker struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int      `json:"id"`
}

type BinanceTicker struct {
	U    int64  `json:"u"`
	S    string `json:"s"`
	B    string `json:"b"`
	BQty string `json:"B"`
	A    string `json:"a"`
	AQty string `json:"A"`
}

func (b *BinanceTicker) String() string {
	return fmt.Sprintf("[Binance] %s Bid: %s Ask: %s", b.S, b.B, b.A)
}


func (b *BinanceTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(b.B, 64)
	ask, _ := strconv.ParseFloat(b.A, 64)
	tickerBuffer[Binance][symbolId].Ask = ask
	tickerBuffer[Binance][symbolId].Bid = bid
}

type SubscribeBybitTicker struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
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

func (b *BybitTicker) String() string {
	sb := strings.Builder{}
	sb.WriteString("[Bybit] ")
	sb.WriteString(b.Data.S)
	sb.WriteString(" Bid: ")
	if len(b.Data.B) > 0 {
		sb.WriteString(b.Data.B[0][0])
	} else {
		sb.WriteString("N/A")
	}
	sb.WriteString(" Ask: ")
	if len(b.Data.A) > 0 {
		sb.WriteString(b.Data.A[0][0])
	} else {
		sb.WriteString("N/A")
	}
	return sb.String()
}

func (b *BybitTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(b.Data.B[0][0], 64)
	ask, _ := strconv.ParseFloat(b.Data.A[0][0], 64)
	tickerBuffer[Bybit][symbolId].Ask = ask
	tickerBuffer[Bybit][symbolId].Bid = bid
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

func (c *CoinbaseTicker) String() string {
	return fmt.Sprintf("[Coinbase] %s Bid: %s Ask: %s", c.ProductID, c.BestBid, c.BestAsk)
}

func (c *CoinbaseTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(c.BestBid, 64)
	ask, _ := strconv.ParseFloat(c.BestAsk, 64)
	tickerBuffer[Coinbase][symbolId].Ask = ask
	tickerBuffer[Coinbase][symbolId].Bid = bid
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

func (g *GateioTicker) String() string {
	return fmt.Sprintf("[Gateio] %s Bid: %s Ask: %s", g.Result.S, g.Result.B, g.Result.A)
}

func (g *GateioTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(g.Result.B, 64)
	ask, _ := strconv.ParseFloat(g.Result.A, 64)
	tickerBuffer[Gateio][symbolId].Ask = ask
	tickerBuffer[Gateio][symbolId].Bid = bid
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

func (h *HTXTicker) String() string {
	return fmt.Sprintf("[HTX] %s Bid: %f Ask: %f", strings.ToUpper(h.Tick.Symbol), h.Tick.Bid, h.Tick.Ask)
}

func (h *HTXTicker) save(symbolId int) {
	tickerBuffer[HTX][symbolId].Ask = h.Tick.Ask
	tickerBuffer[HTX][symbolId].Bid = h.Tick.Bid
}

type SubscribeKucoinTicker struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
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

func (k *KucoinTicker) String() string {
	symbol := strings.Split(k.Topic, ":")[1]
	return fmt.Sprintf("[Kucoin] %s Bid: %s Ask: %s", symbol, k.Data.BestBid, k.Data.BestAsk)
}

func (k *KucoinTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(k.Data.BestBid, 64)
	ask, _ := strconv.ParseFloat(k.Data.BestAsk, 64)
	tickerBuffer[Kucoin][symbolId].Ask = ask
	tickerBuffer[Kucoin][symbolId].Bid = bid
}

type SubscribeMexcTicker struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type MexcTicker struct {
	pb.BookTicker
}

func (m *MexcTicker) String() string {
	data := m.Publicbookticker
	return fmt.Sprintf("[Mexc] %s Bid: %s Ask: %s", m.Symbol, data.BidPrice, data.AskPrice)
}
func (m *MexcTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(m.Publicbookticker.BidPrice, 64)
	ask, _ := strconv.ParseFloat(m.Publicbookticker.AskPrice, 64)
	tickerBuffer[Mexc][symbolId].Ask = ask
	tickerBuffer[Mexc][symbolId].Bid = bid
}

type OKXArg struct {
	Channel string `json:"channel"`
	InstId  string `json:"instId"`
}

type SubscribeOKXTicker struct {
	Op   string   `json:"op"`
	Args []OKXArg `json:"args"`
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

func (o *OKXTicker) String() string {
	symbol := strings.ReplaceAll(o.Arg.InstId, "-", "")
	bid := o.Data[0].Bids[0][0]
	ask := o.Data[0].Asks[0][0]
	return fmt.Sprintf("[OKX] %s Bid: %s Ask: %s", symbol, bid, ask)
}

func (o *OKXTicker) save(symbolId int) {
	bid, _ := strconv.ParseFloat(o.Data[0].Bids[0][0], 64)
	ask, _ := strconv.ParseFloat(o.Data[0].Asks[0][0], 64)
	tickerBuffer[OKX][symbolId].Ask = ask
	tickerBuffer[OKX][symbolId].Bid = bid
}
