package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

// {"stream":"btcusdt@bookTicker","data":{"u":61162952423,"s":"BTCUSDT","b":"97614.
// 93000000","B":"0.10666000","a":"97614.94000000","A":"4.78106000"}}
type BinanceSpotBookTicker struct {
	Stream string `json:"stream"`
	Data   struct {
		U    int64  `json:"u"`
		S    string `json:"s"`
		B    string `json:"b"`
		BQty string `json:"B"`
		A    string `json:"a"`
		AQty string `json:"A"`
	} `json:"data"`
}

func (b *BinanceSpotBookTicker) Symbol() string {
	return b.Data.S
}

func (b *BinanceSpotBookTicker) Bid() string {
	return b.Data.B
}

func (b *BinanceSpotBookTicker) Ask() string {
	return b.Data.A
}

type BinanceWebsocketClient struct {
	conn         *websocket.Conn
	log          *log.Logger
	done         chan struct{}
	should_close chan struct{}
}

func NewBinanceWebsocketClient(base_url string) (*BinanceWebsocketClient, error) {
	log := log.New(os.Stdout, "[BINANCE] ", log.LstdFlags)
	conn, _, err := websocket.DefaultDialer.Dial(base_url, nil)
	if err != nil {
		log.Fatal("dial:", err)
		return nil, err
	}
	c := &BinanceWebsocketClient{conn: conn, log: log, done: make(chan struct{})}
	c.log.Printf("connecting to %s", base_url)
	return c, err
}

func (c *BinanceWebsocketClient) Close() {
	c.conn.Close()
}

func (c *BinanceWebsocketClient) SubscribeBookTickers(symbols []string) error {
	c.log.Printf("subscribing to %s", symbols)
	connection_msg := `{"method":"SUBSCRIBE","params":[`
	for i, symbol := range symbols {
		if i > 0 {
			connection_msg += ","
		}
		connection_msg += `"` + symbol + `@bookTicker"`
	}
	connection_msg += `],"id":1}`

	err := c.conn.WriteMessage(websocket.TextMessage, []byte(connection_msg))

	if err != nil {
		c.log.Println("write:", err)
	}

	_, ack, err := c.conn.ReadMessage()
	if err != nil {
		c.log.Println("read:", err)
		return err
	}

	if string(ack) != `{"result":null,"id":1}` {
		c.log.Println("ack:", string(ack))
		return fmt.Errorf("ack: %s", ack)
	}
	return err

}

func (c *BinanceWebsocketClient) Listen(callback func([]byte)) {
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			c.log.Println("read:", err)
			return
		}
		callback(message)
	}
}

func (c *BinanceWebsocketClient) RunForever(callback func([]byte)) {
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

func main_binance(should_close chan struct{}) {
	if binance_client, err := NewBinanceWebsocketClient("wss://stream.binance.com:9443/stream"); err == nil {
		binance_client.should_close = should_close
		defer binance_client.Close()
		symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
		if binance_client.SubscribeBookTickers(symbols) == nil {
			var prev_msgs map[string]*BinanceSpotBookTicker = make(map[string]*BinanceSpotBookTicker)
			binance_client.RunForever(func(message []byte) {
				var bookTicker BinanceSpotBookTicker
				err = json.Unmarshal(message, &bookTicker)
				if err != nil {
					binance_client.log.Println("unmarshal:", err)
					return
				}
				prev_msg := prev_msgs[bookTicker.Symbol()]
				if prev_msg == nil || prev_msg.Bid() != bookTicker.Bid() || prev_msg.Ask() != bookTicker.Ask() {
					fmt.Printf("[BINANCE] %s: %s %s\n", bookTicker.Symbol(), bookTicker.Ask(), bookTicker.Bid())
				}
				prev_msgs[bookTicker.Symbol()] = &bookTicker
			})
		}
	}
}
