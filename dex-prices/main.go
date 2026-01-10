package main

import (
	"log"
	"sync"
)

func main() {
	// Load configuration from config.json
	config, err := LoadConfig()
	if err != nil {
		log.Printf("⚠ Failed to load config.json: %v", err)
		log.Println("Creating default config.json...")
		if err := CreateDefaultConfig(); err != nil {
			log.Fatalf("❌ Failed to create default config: %v", err)
		}
		log.Println("Please edit config.json and restart the program")
		return
	}

	enabledChains := GetEnabledChains(config)

	if len(enabledChains) == 0 {
		log.Fatal("❌ No chains enabled! Please enable at least one chain in config.json")
	}

	log.Printf("🚀 Starting multi-chain DEX listener for %d chains", len(enabledChains))
	for _, chain := range enabledChains {
		if chain.HasAddressFilter() {
			log.Printf("   • %s (Chain ID: %d) - Filtering %d address(es)", chain.Name, chain.ChainID, len(chain.FilterAddresses))
		} else {
			log.Printf("   • %s (Chain ID: %d) - Monitoring all addresses", chain.Name, chain.ChainID)
		}
	}
	log.Println()

	var wg sync.WaitGroup

	// Start a listener for each enabled chain
	for _, config := range enabledChains {
		wg.Add(1)

		// Route to appropriate listener based on chain type
		if config.ChainType == "solana" {
			go func(cfg ChainConfig) {
				defer wg.Done()
				startSolanaListener(cfg)
			}(config)
		} else {
			// Default to EVM listener for "evm" or unspecified chain types
			go func(cfg ChainConfig) {
				defer wg.Done()
				startEVMListener(cfg)
			}(config)
		}
	}

	// Wait for all listeners (runs indefinitely)
	wg.Wait()
}
