package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type DatabaseWriter struct {
	db *sql.DB
}

func NewDatabaseWriter(connStr string) (*DatabaseWriter, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Connected to PostgreSQL")
	return &DatabaseWriter{db: db}, nil
}

func (w *DatabaseWriter) InsertPriceIndex(symbol string, price float64, numExchanges int, bidVWAP, askVWAP, bidQtyTotal, askQtyTotal float64) error {
	query := `
        INSERT INTO price_index (time, symbol, price_index, num_exchanges, bid_vwap, ask_vwap, bid_qty_total, ask_qty_total)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `
	_, err := w.db.Exec(query, time.Now(), symbol, price, numExchanges, bidVWAP, askVWAP, bidQtyTotal, askQtyTotal)
	if err != nil {
		return fmt.Errorf("failed to insert price index: %w", err)
	}
	return nil
}

type TickerData struct {
	Exchange string
	Symbol   string
	Bid      float64
	Ask      float64
}

func (w *DatabaseWriter) Close() error {
	return w.db.Close()
}
