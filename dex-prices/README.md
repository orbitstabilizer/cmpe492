# DEX Prices Listener

Real-time multi-chain DEX swap monitoring.

## Features

- Multi-chain monitoring across EVM chains
- Uniswap V2/V3/V4, Curve, Balancer support
- WebSocket event subscriptions
- Token metadata caching
- Address filtering

## Quick Start

```bash
# Install dependencies
go mod download

# Run
go run .

# Build
go build -o dex-prices
./dex-prices
```

## Configuration

Edit `config.json`:

```json
{
  "chains": [
    {
      "chainId": 1,
      "name": "Ethereum",
      "chainType": "evm",
      "rpcWs": "wss://ethereum-rpc.publicnode.com",
      "rpcHttp": "https://ethereum-rpc.publicnode.com",
      "nativeToken": {
        "address": "0x0000000000000000000000000000000000000000",
        "symbol": "ETH",
        "decimals": 18
      },
      "enabled": true,
      "filterAddresses": []
    }
  ]
}
```

Set `enabled: false` to disable a chain. Add addresses to `filterAddresses` to monitor specific contracts only.

## Output

```
[Ethereum] Swap (V2) | USDT/WETH | In: 100000000 USDT -> Out: 35000000000000000 WETH | Tx: 0xabcd...
[BSC] Swap (V3) | WBTC/USDC | In: 100000000 WBTC -> Out: 9150000000 USDC | Tx: 0xef01...
```

## Future Improvements

- Solana support
- Price impact calculation
- MEV detection


- [ ] Price impact calculation
- [ ] MEV detection
- [ ] Historical swap data storage
