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

func (w *DatabaseWriter) InsertPriceIndex(symbol string, price float64, numExchanges int) error {
	query := `
        INSERT INTO price_index (time, symbol, price_index, num_exchanges)
        VALUES ($1, $2, $3, $4)
    `
	_, err := w.db.Exec(query, time.Now(), symbol, price, numExchanges)
	if err != nil {
		return fmt.Errorf("failed to insert price index: %w", err)
	}
	return nil
}

func (w *DatabaseWriter) InsertTickers(tickers []TickerData) error {
	if len(tickers) == 0 {
		return nil
	}

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO cex_tickers (time, exchange, symbol, bid, ask)
        VALUES ($1, $2, $3, $4, $5)
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	for _, ticker := range tickers {
		_, err := stmt.Exec(now, ticker.Exchange, ticker.Symbol, ticker.Bid, ticker.Ask)
		if err != nil {
			log.Printf("⚠ Failed to insert ticker: %v", err)
			continue
		}
	}

	return tx.Commit()
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
