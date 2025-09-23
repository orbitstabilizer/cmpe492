package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// {"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"asks":[["97687.6","0.00445824","0","3
// "]],"bids":[["97687.5","0.84129164","0","10"]],"ts":"1739657990337","seqId":45774132816}]}

type OKXSpotBookTicker struct {
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

func (b *OKXSpotBookTicker) Symbol() string {
	// "BTC-USDT" -> "BTCUSDT"
	base_quote := strings.Split(b.Arg.InstId, "-")
	return base_quote[0] + base_quote[1]
}

func (b *OKXSpotBookTicker) Bid() string {
	return b.Data[0].Bids[0][0]
}

func (b *OKXSpotBookTicker) Ask() string {
	return b.Data[0].Asks[0][0]
}

type OKXWebsocketClient struct {
	conn         *websocket.Conn
	log          *log.Logger
	done         chan struct{}
	should_close chan struct{}
}

func NewOKXWebsocketClient(base_url string) (*OKXWebsocketClient, error) {
	log := log.New(os.Stdout, "[OKX] ", log.LstdFlags)
	conn, _, err := websocket.DefaultDialer.Dial(base_url, nil)
	if err != nil {
		log.Fatal("dial:", err)
		return nil, err
	}
	c := &OKXWebsocketClient{conn: conn, log: log, done: make(chan struct{})}
	c.log.Printf("connecting to %s", base_url)
	return c, err
}

func (c *OKXWebsocketClient) Close() {
	c.conn.Close()
}

func (c *OKXWebsocketClient) SubscribeBookTickers(symbols []string) error {
	c.log.Printf("subscribing to %s", symbols)
	subscription_request := map[string]interface{}{
		"op":   "subscribe",
		"args": []map[string]string{
			// {"channel": "bbo-tbt", "instId": "BTC-USDT"},
		},
	}
	for _, symbol := range symbols {
		subscription_request["args"] = append(subscription_request["args"].([]map[string]string), map[string]string{"channel": "bbo-tbt", "instId": symbol})
	}

	if err := c.conn.WriteJSON(subscription_request); err != nil {
		c.log.Fatal(err)
	}
	for _, symbol := range symbols {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			c.log.Fatal(err)
		}
		c.log.Printf("Received: %s, %s\n", symbol, message)
	}
	return nil
}

func (c *OKXWebsocketClient) Listen(callback func([]byte)) {
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			c.log.Println("read:", err)
			return
		}
		callback(message)
	}
}

func (c *OKXWebsocketClient) RunForever(callback func([]byte)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	go c.Listen(callback)
	for {
		select {
		case <-c.done:
			return
		// case t := <-ticker.C:
		// log.Println("Ticker at", t)
		case <-c.should_close:
			c.log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				c.log.Println("write close:", err)
			}
			select {
			case <-c.done:
			case <-time.After(time.Second):
			}
			c.log.Println("closing")
			return
		}
	}

}

func main_okx(should_close chan struct{}) {
	if okx_client, err := NewOKXWebsocketClient("wss://ws.okx.com:8443/ws/v5/public"); err == nil {
		okx_client.should_close = should_close
		defer okx_client.Close()
		symbols := []string{"BTC-USDT", "ETH-USDT", "BNB-USDT"}
		if okx_client.SubscribeBookTickers(symbols) == nil {
			var prev_msgs map[string]*OKXSpotBookTicker = make(map[string]*OKXSpotBookTicker)
			okx_client.RunForever(func(message []byte) {
				fmt.Println(string(message))
				var bookTicker OKXSpotBookTicker
				err = json.Unmarshal(message, &bookTicker)
				if err != nil {
					okx_client.log.Println("unmarshal:", err)
					return
				}
				prev_msg := prev_msgs[bookTicker.Symbol()]
				if prev_msg == nil || prev_msg.Bid() != bookTicker.Bid() || prev_msg.Ask() != bookTicker.Ask() {
					fmt.Printf("[    OKX] %s: %s %s\n", bookTicker.Symbol(), bookTicker.Ask(), bookTicker.Bid())
				}
				prev_msgs[bookTicker.Symbol()] = &bookTicker
			})
		}
	}
}
