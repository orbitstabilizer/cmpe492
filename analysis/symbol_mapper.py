"""
Symbol Mapping Utility
Handles conversion between DEX token symbols and CEX price index symbols
"""

import logging
from typing import Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SymbolMapper:
    """Maps between DEX token symbols (WETH, WBTC) and CEX symbols (eth, btc)"""
    
    # Mapping from DEX token symbols to normalized base symbols
    TOKEN_MAPPING = {
        # Ethereum tokens
        'WETH': 'eth',
        'ETH': 'eth',
        
        # Bitcoin variants
        'WBTC': 'btc',
        'BTCB': 'btc',
        'BTC': 'btc',
        
        # BNB variants
        'WBNB': 'bnb',
        'BNB': 'bnb',
        
        # Stablecoins
        'USDT': 'usdt',
        'USDC': 'usdc',
        'BUSD': 'busd',
        'DAI': 'dai',
    }
    
    @staticmethod
    def normalize_token_symbol(token_symbol: str) -> str:
        """
        Normalize a DEX token symbol to CEX format
        
        Args:
            token_symbol: Token symbol from DEX (e.g., "WETH", "WBTC")
            
        Returns:
            Normalized symbol in lowercase (e.g., "eth", "btc")
        """
        # Remove whitespace and convert to uppercase for lookup
        token_symbol = token_symbol.strip().upper()
        
        # Try direct mapping first
        if token_symbol in SymbolMapper.TOKEN_MAPPING:
            return SymbolMapper.TOKEN_MAPPING[token_symbol]
        
        # If no mapping found, return lowercase version
        logger.warning(f"No mapping found for token '{token_symbol}', using lowercase")
        return token_symbol.lower()
    
    @staticmethod
    def create_price_index_symbol(token0_symbol: str, token1_symbol: str) -> str:
        """
        Create price index symbol from two token symbols
        
        Args:
            token0_symbol: First token symbol (e.g., "WETH")
            token1_symbol: Second token symbol (e.g., "USDT")
            
        Returns:
            Price index symbol (e.g., "ethusdt")
        """
        norm0 = SymbolMapper.normalize_token_symbol(token0_symbol)
        norm1 = SymbolMapper.normalize_token_symbol(token1_symbol)
        
        # Create symbol like "ethusdt"
        return f"{norm0}{norm1}"
    
    @staticmethod
    def parse_price_index_symbol(symbol: str) -> Optional[Tuple[str, str]]:
        """
        Parse a price index symbol into base and quote
        
        Args:
            symbol: Price index symbol (e.g., "ethusdt", "btcusdt")
            
        Returns:
            Tuple of (base, quote) or None if cannot parse
        """
        symbol_lower = symbol.lower()
        
        # Common quote currencies
        quotes = ['usdt', 'usdc', 'busd', 'dai']
        
        for quote in quotes:
            if symbol_lower.endswith(quote):
                base = symbol_lower[:-len(quote)]
                return (base, quote)
        
        return None
    
    @staticmethod
    def match_pool_to_price_index(
        token0_symbol: str, 
        token1_symbol: str
    ) -> Optional[str]:
        """
        Match a DEX pool (token0/token1) to a CEX price index symbol
        
        Args:
            token0_symbol: First token symbol from DEX
            token1_symbol: Second token symbol from DEX
            
        Returns:
            Price index symbol or None if no match
        """
        # Normalize both symbols
        norm0 = SymbolMapper.normalize_token_symbol(token0_symbol)
        norm1 = SymbolMapper.normalize_token_symbol(token1_symbol)
        
        # Common stablecoins
        stablecoins = {'usdt', 'usdc', 'busd', 'dai'}
        
        # Determine which is the base and which is the quote
        if norm1 in stablecoins:
            # token1 is stablecoin (quote), token0 is base
            return f"{norm0}{norm1}"
        elif norm0 in stablecoins:
            # token0 is stablecoin (quote), token1 is base
            return f"{norm1}{norm0}"
        else:
            # Neither is a stablecoin, try both orders
            # Prefer the first one
            logger.warning(
                f"No stablecoin found in pair {token0_symbol}/{token1_symbol}, "
                f"using {norm0}{norm1}"
            )
            return f"{norm0}{norm1}"
    
    @staticmethod
    def get_reverse_symbol(symbol: str) -> str:
        """
        Get the reverse of a price index symbol
        
        Args:
            symbol: Price index symbol (e.g., "ethusdt")
            
        Returns:
            Reversed symbol (e.g., "usdteth")
        """
        parsed = SymbolMapper.parse_price_index_symbol(symbol)
        if parsed:
            base, quote = parsed
            return f"{quote}{base}"
        
        # If can't parse, just reverse the string
        return symbol[::-1]
    
    @staticmethod
    def is_equivalent_pair(
        pool_token0: str,
        pool_token1: str,
        index_symbol: str
    ) -> bool:
        """
        Check if a pool matches a price index symbol
        
        Args:
            pool_token0: First token symbol from pool
            pool_token1: Second token symbol from pool
            index_symbol: Price index symbol to match against
            
        Returns:
            True if the pool matches the index symbol
        """
        matched = SymbolMapper.match_pool_to_price_index(pool_token0, pool_token1)
        if not matched:
            return False
        
        # Check both forward and reverse
        index_lower = index_symbol.lower()
        reverse = SymbolMapper.get_reverse_symbol(index_lower)
        
        return matched == index_lower or matched == reverse


# Example usage and tests
if __name__ == "__main__":
    mapper = SymbolMapper()
    
    # Test normalizations
    print("Testing normalizations:")
    print(f"WETH → {mapper.normalize_token_symbol('WETH')}")  # eth
    print(f"WBTC → {mapper.normalize_token_symbol('WBTC')}")  # btc
    print(f"BTCB → {mapper.normalize_token_symbol('BTCB')}")  # btc
    print(f"WBNB → {mapper.normalize_token_symbol('WBNB')}")  # bnb
    print(f"USDT → {mapper.normalize_token_symbol('USDT')}")  # usdt
    print()
    
    # Test price index symbol creation
    print("Testing price index symbol creation:")
    print(f"WETH/USDT → {mapper.create_price_index_symbol('WETH', 'USDT')}")  # ethusdt
    print(f"WBTC/USDT → {mapper.create_price_index_symbol('WBTC', 'USDT')}")  # btcusdt
    print(f"BTCB/USDT → {mapper.create_price_index_symbol('BTCB', 'USDT')}")  # btcusdt
    print(f"WBNB/USDT → {mapper.create_price_index_symbol('WBNB', 'USDT')}")  # bnbusdt
    print()
    
    # Test matching
    print("Testing pool to price index matching:")
    print(f"WETH/USDT → {mapper.match_pool_to_price_index('WETH', 'USDT')}")  # ethusdt
    print(f"USDT/WETH → {mapper.match_pool_to_price_index('USDT', 'WETH')}")  # ethusdt
    print(f"BTCB/USDT → {mapper.match_pool_to_price_index('BTCB', 'USDT')}")  # btcusdt
    print(f"WBNB/USDT → {mapper.match_pool_to_price_index('WBNB', 'USDT')}")  # bnbusdt
    print()
    
    # Test equivalence checking
    print("Testing equivalence:")
    print(f"WETH/USDT matches 'ethusdt': {mapper.is_equivalent_pair('WETH', 'USDT', 'ethusdt')}")  # True
    print(f"USDT/WETH matches 'ethusdt': {mapper.is_equivalent_pair('USDT', 'WETH', 'ethusdt')}")  # True
    print(f"WBTC/USDT matches 'btcusdt': {mapper.is_equivalent_pair('WBTC', 'USDT', 'btcusdt')}")  # True
    print(f"BTCB/USDT matches 'btcusdt': {mapper.is_equivalent_pair('BTCB', 'USDT', 'btcusdt')}")  # True

