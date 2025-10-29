# Uniswap V4 Swap Event Listener for BSC

A Python script to monitor and display swap events from Uniswap V4 pools on BNB Smart Chain in real-time.

## Overview

This tool listens to `Swap` events emitted by the Uniswap V4 PoolManager contract on BSC Mainnet. It displays detailed information about each swap including amounts, prices, liquidity, and pool information.

### Monitored Contract

- **Network**: BNB Smart Chain (Chain ID: 56)
- **PoolManager Address**: `0x28e2ea090877bf75740558f6bfb36a5ffee9e9df`

## Features

- **Real-time Monitoring**: Listen to swap events as they happen
- **Historical Events**: Fetch past swap events from specific block ranges
- **Pool Filtering**: Monitor swaps for specific pools only
- **Token Information**: Automatically fetches and displays:
  - Token symbols and names (e.g., "WBNB", "USDT")
  - Human-readable amounts with proper decimals
  - Token contract addresses
- **Pool Information**: Decodes Pool IDs to show:
  - Trading pair (e.g., "WBNB/USDT")
  - Token addresses
  - Pool fee tier
- **Smart Caching**: Caches token and pool metadata to minimize RPC calls
- **Detailed Information**: View all swap parameters including:
  - Formatted swap display (e.g., "0.5 WBNB → 150 USDT")
  - Raw and formatted token amounts
  - Current price (calculated from sqrtPriceX96)
  - Liquidity
  - Current tick
  - Swap fee

## Setup

### 1. Install Python Dependencies

```bash
cd swap-listener
pip install -r requirements.txt
```

### 2. Configure RPC Endpoint (Optional)

Copy the example environment file and configure your RPC endpoint:

```bash
cp .env.example .env
```

Edit `.env` and set your preferred BSC RPC URL. The script uses a public endpoint by default, but for production use, consider using a dedicated RPC provider like:

- [NodeReal](https://nodereal.io/) (MegaNode)
- [Ankr](https://www.ankr.com/)
- [QuickNode](https://www.quicknode.com/)

## Usage

Make the script executable:

```bash
chmod +x listen_swaps.py
```

### Real-time Monitoring

Monitor all swaps in real-time (default mode):

```bash
python listen_swaps.py
# or
python listen_swaps.py realtime
```

### Historical Events

Fetch swap events from a specific block range:

```bash
# Last 1000 blocks
python listen_swaps.py historical

# Specific block range
python listen_swaps.py historical <from_block> <to_block>

# Example: blocks 35000000 to 35001000
python listen_swaps.py historical 35000000 35001000
```

### Monitor Specific Pool

Listen to swaps for a specific pool ID:

```bash
# Historical events only
python listen_swaps.py pool <pool_id> [from_block]

# Historical + real-time monitoring
python listen_swaps.py pool <pool_id> [from_block] --realtime

# Example
python listen_swaps.py pool 0x1234...abcd 35000000 --realtime
```

## Output Format

Each swap event is displayed with detailed token information:

```
================================================================================
Block: 66254060 | Tx: 53af32e23aaece44ed46d919b5f977196c27f972a94d9acc0e4e1a58176e2f86
Pool: USDT/WBNB (Fee: 0.05%)
Pool ID: c197357b0f65a134cf443d8fbbd77b3070861514a9eb3f9162620a6452d1b59f
Sender: 0x1906c1d672b88cD1B9aC7593301cA990F94Eae07

SWAP: 0.264082 USDT → 0.027107 WBNB

Token Details:
  Token0: USDT (Tether USD)
          0x55d398326f99059fF775485246999027B3197955
          Amount: 0.027107 (27,107,199,028,502,368 raw)

  Token1: WBNB (Wrapped BNB)
          0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c
          Amount: 0.264082 (-264,082,092,750,503 raw)

Pool State:
  Sqrt Price X96: 7818097084609547986604960536
  Price: 0.009737398659608478
  Liquidity: 198,784,079,781,894,592,691
  Tick: -46321
================================================================================
```

### Understanding the Output

- **Block**: Block number where the swap occurred
- **Tx**: Transaction hash (can be viewed on BSCScan)
- **Pool**: Trading pair with token symbols and fee tier
- **Pool ID**: Unique identifier for the pool (hash of PoolKey struct)
- **Sender**: Address that initiated the swap (often the Universal Router)
- **SWAP**: Human-readable swap summary showing input → output with token symbols
- **Token Details**: Full information for each token including:
  - Symbol and name
  - Contract address
  - Formatted amount (with proper decimals)
  - Raw amount (as stored in contract)
- **Pool State**: Current state after swap
  - **Sqrt Price X96**: Price as Q64.96 fixed-point number
  - **Price**: Approximate price ratio
  - **Liquidity**: Total liquidity in the pool
  - **Tick**: Current tick position

### Understanding token0 and token1

In Uniswap V4, every pool has two tokens that are **sorted by their contract addresses**:
- **token0**: Token with the lower address (alphabetically/numerically)
- **token1**: Token with the higher address

This is just an ordering convention and doesn't indicate which is the "base" or "quote" token. The script automatically shows you which actual tokens these are (e.g., WBNB, USDT) so you don't need to worry about the ordering.

## Swap Event Structure

The `Swap` event from the PoolManager contract has the following parameters:

```solidity
event Swap(
    PoolId indexed id,           // Pool identifier
    address indexed sender,       // Address that initiated the swap
    int128 amount0,              // Delta of currency0
    int128 amount1,              // Delta of currency1
    uint160 sqrtPriceX96,        // Square root price after swap
    uint128 liquidity,           // Liquidity after swap
    int24 tick,                  // Tick after swap
    uint24 fee                   // Swap fee in hundredths of a bip
);
```

## Tips

1. **First Swap Per Pool**: The first time a swap is detected for a new pool, the script will search for the pool's `Initialize` event to fetch token information. This may take a few seconds but subsequent swaps from the same pool will be instant due to caching.

2. **Rate Limiting**: Public RPC endpoints may have rate limits. For heavy usage, use a dedicated RPC provider.

3. **Block Range**: When fetching historical events, use smaller block ranges (e.g., 1000-5000 blocks) to avoid timeouts.

4. **Finding Pool IDs**: Pool IDs are computed as the hash of the PoolKey struct. The script automatically decodes them to show you the actual tokens, but you can also:
   - Listen to `Initialize` events to discover new pools
   - Monitor all swaps and note interesting pool IDs
   - Calculate pool ID from known pool parameters

5. **Network Delays**: Real-time monitoring polls every 2 seconds by default. Adjust the `poll_interval` in the code if needed.

## Troubleshooting

### Connection Issues

If you get connection errors:
- Check your internet connection
- Try a different RPC endpoint from `.env.example`
- Verify BSC is not experiencing network issues

### No Events Found

If no events are returned:
- Verify the block range contains swap transactions
- Check if Uniswap V4 is active on BSC at those blocks
- Try a more recent block range

### Rate Limiting

If you see rate limiting errors:
- Switch to a different RPC endpoint
- Add delays between requests
- Use a paid RPC provider for higher limits

## BSC Contract Addresses

For reference, here are the Uniswap V4 deployment addresses on BSC:

| Contract | Address |
|----------|---------|
| PoolManager | `0x28e2ea090877bf75740558f6bfb36a5ffee9e9df` |
| PositionDescriptor | `0xf0432f360703ec3d33931a8356a75a77d8d380e1` |
| PositionManager | `0x7a4a5c919ae2541aed11041a1aeee68f1287f95b` |
| Quoter | `0x9f75dd27d6664c475b90e105573e550ff69437b0` |
| StateView | `0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4` |
| Universal Router | `0x1906c1d672b88cd1b9ac7593301ca990f94eae07` |
| Permit2 | `0x000000000022D473030F116dDEE9F6B43aC78BA3` |

## License

This tool is provided as-is for educational and monitoring purposes.
