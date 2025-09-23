package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	should_close_okx := make(chan struct{})
	should_close_binance := make(chan struct{})
	go main_okx(should_close_okx)
	go main_binance(should_close_binance)
	<-interrupt
	fmt.Println("interrupt")
	should_close_okx <- struct{}{}
	should_close_binance <- struct{}{}
	<-time.After(time.Second * 2)

}
