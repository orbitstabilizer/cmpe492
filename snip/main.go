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
	return [...]string{"Binance", "Bybit", "Coinbase", "Gateio", "HTX", "Kucoin", "Mexc", "OKX"}[e]
}

const NUM_SYMBOLS = 3

type TickerData struct {
	Bid float64
	Ask float64
}
type TickerBuffer = [NUM_EXCHANGES][NUM_SYMBOLS]TickerData

var tickerBuffer *TickerBuffer

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
			// println(ticker.String())
			tickId := -1
			for i, symbol := range symbols {
				if ticker.Arg.InstId == symbol {
					tickId = i
					break
				}
			}
			if tickId != -1 {
				ticker.save(tickId)
			}
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
		// println(ticker.String())
		tickId := -1
		for i, symbol := range symbols {
			if ticker.Symbol == symbol {
				tickId = i
				break
			}
		}
		if tickId != -1 {
			ticker.save(tickId)
		}
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
			// println(ticker.String())
			tickId := -1
			for i, symbol := range symbols {
				if ticker.Topic == "/market/ticker:"+symbol {
					tickId = i
					break
				}
			}
			if tickId != -1 {
				ticker.save(tickId)
			}
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
			// fmt.Println(ticker.String())
			tickId := -1
			for i, symbol := range symbols {
				if ticker.Tick.Symbol == symbol {
					tickId = i
					break
				}
			}
			if tickId != -1 {
				ticker.save(tickId)
			}
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
		// println(ticker.String())
		tickId := -1
		for i, symbol := range symbols {
			if ticker.Result.S == symbol {
				tickId = i
				break
			}
		}
		if tickId != -1 {
			ticker.save(tickId)
		}
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
			// println(ticker.ProductID + " Bid: " + ticker.BestBid + " Ask: " + ticker.BestAsk)
			tickId := -1
			for i, symbol := range symbols {
				if ticker.ProductID == symbol {
					tickId = i
					break
				}
			}
			if tickId != -1 {
				ticker.save(tickId)
			}
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
		// println(ticker.String())
		tickId := -1
		for i, symbol := range symbols {
			// ticker.S is upper case
			if ticker.S == strings.ToUpper(symbol) {
				tickId = i
				break
			}
		}
		if tickId != -1 {
			ticker.save(tickId)
		}
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
		// println(ticker.String())
		tickId := -1
		for i, symbol := range symbols {
			if ticker.Data.S == symbol {
				tickId = i
				break
			}
		}
		if tickId != -1 {
			ticker.save(tickId)
		}

	}
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	shouldClose := false

	w, err := NewSHMWriter[TickerBuffer](".ticker.data")
	defer w.Close()
	if err != nil {
		panic(err)
	}
	tickerBuffer = w.Data

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
