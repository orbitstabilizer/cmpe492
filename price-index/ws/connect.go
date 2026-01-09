package ws

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	. "price-index/schema"
)

const (
	BINANCE_WS_URL  = "wss://stream.binance.com:9443/ws"
	BYBIT_WS_URL    = "wss://stream.bybit.com/v5/public/spot"
	COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
	GATEIO_WS_URL   = "wss://api.gateio.ws/ws/v4/"
	HTX_WS_URL      = "wss://api-aws.huobi.pro/ws"
	KUCOIN_WS_URL   = "wss://ws-api.kucoin.com/endpoint"
	MEXC_WS_URL     = "wss://wbs-api.mexc.com/ws"
	OKX_WS_URL      = "wss://ws.okx.com:8443/ws/v5/public"
)

var updateChan chan int

func connectOKX(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i
	}
RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(OKX_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

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
			log.Println("OKX read error:", err)
			goto RECONNECT
		}
		ticker := OKXTicker{}
		if err := json.Unmarshal(message, &ticker); err != nil {
			// This might fail on non-data messages like heartbeat
			continue
		}
		if len(ticker.Data) > 0 {
			if tickId, ok := symbolToTick[ticker.Arg.InstId]; ok {
				tickerConntainer[tickId].Parse(&ticker)
				updateChan <- tickId
				continue
			}

		}
	}
}

func connectMexc(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i
	}
RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(MEXC_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

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
			log.Println("Mexc read error:", err)
			goto RECONNECT
		}
		var ticker MexcTicker
		if err := proto.Unmarshal(message, &ticker); err != nil {
			// This might fail on non-data messages like heartbeat
			continue
		}
		if tickId, ok := symbolToTick[ticker.Symbol]; ok {
			tickerConntainer[tickId].Parse(&ticker)
				updateChan <- tickId
			continue
		}

	}
}

func connectKucoin(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick["/market/ticker:"+symbol] = i
	}
RECONNECT:
	//  request WS token
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

	conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

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

	//  start ping loop in background
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

	for !*shouldClose {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("Kucoin read error:", err)
			goto RECONNECT
		}

		var ticker KucoinTicker
		if err := json.Unmarshal(msg, &ticker); err == nil && ticker.Type == "message" {
			if tickId, ok := symbolToTick[ticker.Topic]; ok {
				tickerConntainer[tickId].Parse(&ticker)
				updateChan <- tickId
				continue
			}
		}
	}
}

func connectHTX(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i
	}
RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(HTX_WS_URL, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}()

	// Subscribe to BBO channels
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
			log.Println("HTX read error:", err)
			goto RECONNECT
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
			if tickId, ok := symbolToTick[ticker.Tick.Symbol]; ok {
				tickerConntainer[tickId].Parse(&ticker)
				updateChan <- tickId
				continue
			}

		}
	}
}

func connectGateio(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i
	}
RECONNECT:
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
			log.Println("Gateio read error:", err)
			goto RECONNECT
		}
		ticker := GateioTicker{}
		json.Unmarshal(message, &ticker)
		if tickId, ok := symbolToTick[ticker.Result.S]; ok {
			tickerConntainer[tickId].Parse(&ticker)
			updateChan <- tickId
			continue
		}
	}
}

func connectCoinbase(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i
	}
RECONNECT:
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
			log.Println("Coinbase read error:", err)
			goto RECONNECT

		}
		ticker := CoinbaseTicker{}
		json.Unmarshal(message, &ticker)
		if ticker.Type == "ticker" {
			if tickId, ok := symbolToTick[ticker.ProductID]; ok {
				tickerConntainer[tickId].Parse(&ticker)
				updateChan <- tickId
				continue
			}

		}
	}
}

func connectBinance(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[strings.ToUpper(symbol)] = i
	}
RECONNECT:
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
			log.Println("Binance read error:", err)
			goto RECONNECT
		}
		if tickId, ok := symbolToTick[ticker.S]; ok {
			tickerConntainer[tickId].Parse(&ticker)
			updateChan <- tickId
			continue
		}

	}
}

func connectBybit(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
) {
	const MAX_SYMBOLS_PER_CONN = 10
	totalSymbols := len(symbols)
	for offset := 0; offset < totalSymbols; offset += MAX_SYMBOLS_PER_CONN {
		end := offset + MAX_SYMBOLS_PER_CONN
		if end > totalSymbols {
			end = totalSymbols
		}
		go connectBybitPartial(symbols[offset:end], tickerConntainer, shouldClose, offset)
	}
}

func connectBybitPartial(
	symbols []string,
	tickerConntainer []TickerData,
	shouldClose *bool,
	offset int,
) {
	symbolToTick := map[string]int{}
	for i, symbol := range symbols {
		symbolToTick[symbol] = i + offset
	}
RECONNECT:
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
			log.Println("Bybit read error:", err)
			goto RECONNECT
		}
		ticker := BybitTicker{}
		json.Unmarshal(message, &ticker)
		if tickId, ok := symbolToTick[ticker.Data.S]; ok {
			tickerConntainer[tickId].Parse(&ticker)
			updateChan <- tickId
			continue
		}
	}
}

func ConnectExchange(
	exchange Exchange,
	symbols []string,
	tickerContainer []TickerData,
	shouldClose *bool,
	updateChan_ chan int,
) {
	updateChan = updateChan_
	switch exchange {
	case BINANCE:
		connectBinance(symbols, tickerContainer, shouldClose)
	case BYBIT:
		connectBybit(symbols, tickerContainer, shouldClose)
	case COINBASE:
		connectCoinbase(symbols, tickerContainer, shouldClose)
	case GATEIO:
		connectGateio(symbols, tickerContainer, shouldClose)
	case HTX:
		connectHTX(symbols, tickerContainer, shouldClose)
	case KUCOIN:
		connectKucoin(symbols, tickerContainer, shouldClose)
	case MEXC:
		connectMexc(symbols, tickerContainer, shouldClose)
	case OKX:
		connectOKX(symbols, tickerContainer, shouldClose)
	}
}
