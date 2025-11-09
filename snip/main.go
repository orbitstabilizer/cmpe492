package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"exchange/pb"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var (
	BINANCE_WS_URL  = "wss://stream.binance.com:9443/ws"
	BYBIT_WS_URL    = "wss://stream.bybit.com/v5/public/spot"
	COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
	GATEIO_WS_URL   = "wss://api.gateio.ws/ws/v4/"
	HTX_WS_URL      = "wss://api-aws.huobi.pro/ws"
	KUCOIN_WS_URL   = "wss://ws-api.kucoin.com/endpoint"
	MEXC_WS_URL     = "wss://wbs-api.mexc.com/ws"
	OKX_WS_URL      = "wss://ws.okx.com:8443/ws/v5/public"
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

func connectOKX(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(OKX_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

	symbols := []string{"BTC-USDT", "ETH-USDT", "BNB-USDT"}

	args := make([]OKXArg, len(symbols))
	for i, symbol := range symbols {
		args[i] = OKXArg{Channel: "bbo-tbt", InstId: symbol}
	}

	subscribeMsg := SubscribeOKXTicker{
		Op:   "subscribe",
		Args: args,
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}
		ticker := OKXTicker{}
		if err := json.Unmarshal(message, &ticker); err != nil {
			// This might fail on non-data messages like heartbeat
			continue
		}
		if len(ticker.Data) > 0 {
			println(ticker.String())
		}
	}
}

func connectMexc(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(MEXC_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}
	params := []string{}
	for _, symbol := range symbols {
		params = append(params, fmt.Sprintf("spot@public.aggre.bookTicker.v3.api.pb@10ms@%s", symbol))
	}

	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIPTION",
		"params": params,
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}
		var ticker MexcTicker
		if err := proto.Unmarshal(message, &ticker); err != nil {
			// This might fail on non-data messages like heartbeat
			continue
		}
		println(ticker.String())
	}
}
func connectKucoin(shouldClose *bool) {
	// 1. Request WS token
	resp, err := http.Post("https://api.kucoin.com/api/v1/bullet-public", "application/json", nil)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		panic(err)
	}

	if result["code"] != "200000" {
		panic("Failed to get Kucoin WS token")
	}

	data := result["data"].(map[string]interface{})
	token := data["token"].(string)
	instanceServers := data["instanceServers"].([]interface{})
	serverInfo := instanceServers[0].(map[string]interface{})
	pingInterval := time.Duration(serverInfo["pingInterval"].(float64)) * time.Millisecond
	wsUrl := serverInfo["endpoint"].(string) + "?token=" + token

	// 2. Connect to WS
	conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 3. Subscribe to tickers
	symbols := []string{"BTC-USDT", "ETH-USDT", "BNB-USDT"}
	for _, symbol := range symbols {
		subMsg := SubscribeKucoinTicker{
			ID:             fmt.Sprintf("%d", time.Now().UnixNano()),
			Type:           "subscribe",
			Topic:          fmt.Sprintf("/market/ticker:%s", symbol),
			PrivateChannel: false,
			Response:       true,
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			panic(err)
		}
		time.Sleep(200 * time.Millisecond) // avoid rate limits
	}

	// 4. Start ping loop in background
	go func() {
		ticker := time.NewTicker(time.Duration(float64(pingInterval) * 0.9))
		defer ticker.Stop()
		for !*shouldClose {
			<-ticker.C
			pingMsg := map[string]interface{}{
				"id":   time.Now().UnixMilli(),
				"type": "ping",
			}
			if err := conn.WriteJSON(pingMsg); err != nil {
				fmt.Println("Ping error:", err)
				return
			}
		}
	}()

	// 5. Read loop
	for !*shouldClose {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Println("Read error:", err)
			return
		}

		var ticker KucoinTicker
		if err := json.Unmarshal(msg, &ticker); err == nil && ticker.Type == "message" {
			println(ticker.String())
		}
	}
}
func connectHTX(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(HTX_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

	// Subscribe to BBO channels
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
	for _, symbol := range symbols {
		subscribeMsg := map[string]interface{}{
			"sub": fmt.Sprintf("market.%s.bbo", symbol),
			"id":  "id_" + symbol,
		}
		if err := conn.WriteJSON(subscribeMsg); err != nil {
			panic(err)
		}
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}

		// Decompress gzip payload
		r, err := gzip.NewReader(bytes.NewReader(message))
		if err != nil {
			panic(err)
		}
		decompressed, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			panic(err)
		}

		// Detect and handle ping
		var pingMsg map[string]interface{}
		if err := json.Unmarshal(decompressed, &pingMsg); err == nil {
			if pingVal, ok := pingMsg["ping"]; ok {
				pongMsg := map[string]interface{}{"pong": pingVal}
				if err := conn.WriteJSON(pongMsg); err != nil {
					panic(err)
				}
				continue // skip processing further
			}
		}

		// Parse ticker update
		var ticker HTXTicker
		if err := json.Unmarshal(decompressed, &ticker); err != nil {
			continue
		}
		if ticker.Ch != "" {
			fmt.Println(ticker.String())
		}
	}
}
func connectGateio(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(GATEIO_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			panic(err)
		}
		conn.Close()
	}()

	symbols := []string{"BTC_USDT", "ETH_USDT", "BNB_USDT"}
	subscribeMsg := map[string]interface{}{
		"channel": "spot.book_ticker",
		"event":   "subscribe",
		"payload": symbols,
	}

	err = conn.WriteJSON(subscribeMsg)
	if err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}
		ticker := GateioTicker{}
		json.Unmarshal(message, &ticker)
		println(ticker.String())
	}
}

func connectCoinbase(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(COINBASE_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			panic(err)
		}
		conn.Close()
	}()

	symbols := []string{"BTC-USD", "ETH-USD", "BNB-USD"}
	subscribeMsg := map[string]interface{}{
		"type": "subscribe",
		"channels": []map[string]interface{}{
			{
				"name":        "ticker",
				"product_ids": symbols,
			},
		},
	}

	err = conn.WriteJSON(subscribeMsg)
	if err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}
		ticker := CoinbaseTicker{}
		json.Unmarshal(message, &ticker)
		if ticker.Type == "ticker" {
			println(ticker.ProductID + " Bid: " + ticker.BestBid + " Ask: " + ticker.BestAsk)
		}
	}
}

func connectBinance(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(BINANCE_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			panic(err)
		}
		conn.Close()
	}()

	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
	params := []string{}
	for _, symbol := range symbols {
		params = append(params, symbol+"@bookTicker")
	}
	subscribeMsg := SubscribeBinanceTicker{
		Method: "SUBSCRIBE",
		Params: params,
		ID:     1,
	}

	err = conn.WriteJSON(subscribeMsg)
	if err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		ticker := BinanceTicker{}
		json.Unmarshal(message, &ticker)
		if err != nil {
			panic(err)
		}
		println(ticker.String())
	}
}

func connectBybit(shouldClose *bool) {
	conn, _, err := websocket.DefaultDialer.Dial(BYBIT_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			panic(err)
		}
		conn.Close()
	}()

	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}
	args := []string{}
	for _, symbol := range symbols {
		args = append(args, "orderbook.1."+symbol)
	}
	subscribeMsg := SubscribeBybitTicker{
		Op:   "subscribe",
		Args: args,
	}

	err = conn.WriteJSON(subscribeMsg)
	if err != nil {
		panic(err)
	}

	for !*shouldClose {
		_, message, err := conn.ReadMessage()
		if err != nil {
			panic(err)
		}
		ticker := BybitTicker{}
		json.Unmarshal(message, &ticker)
		println(ticker.String())

	}
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	shouldClose := false
	go connectBinance(&shouldClose)
	go connectBybit(&shouldClose)
	go connectCoinbase(&shouldClose)
	go connectGateio(&shouldClose)
	go connectHTX(&shouldClose)
	go connectKucoin(&shouldClose)
	go connectMexc(&shouldClose)
	go connectOKX(&shouldClose)
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	shouldClose = true
}
