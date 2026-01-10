import pandas as pd
import streamlit as st
from ffi import get_ticker_dfs,get_price_indices_df, Symbols, read_shm
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np
import os

TAB_NAMES = ["Price Index", "Analysis"]
def main():
    refresh_interval = st.sidebar.number_input(
        "Auto-Refresh Interval (seconds/10)", min_value=1, max_value=600, value=1, step=1
    ) * 100  # Convert to milliseconds
    st.markdown("""
        <style>
        stStatusWidget{display:none!important;}
        </style>
    """,unsafe_allow_html=True)

    selected_tab = st.radio("Select a view:", TAB_NAMES, horizontal=True)
    st.session_state["selected_tab"] = selected_tab

    if selected_tab == "Price Index":
        st_autorefresh(interval=refresh_interval, limit=None)

        tab_detatiled, tab_price_index, tab_orderbook = st.tabs(["Detailed View", "Price Index", "Order Book"])
        with tab_detatiled:
            # choose symbol index
            symbol_name = st.selectbox("Select Symbol Index", options=[sym.name for sym in Symbols], index=0)
            price_index = float(df_price_index.loc[symbol_name].iloc[0])
            st.subheader(f"{symbol_name}: Price Index: {price_index:.6f}")
            df_ticker = df_tickers[symbol_name]
            st.dataframe(df_ticker)

        with tab_price_index:
            st.title("Price Index Summary")
            st.dataframe(df_price_index, height=1000)

        with tab_orderbook:
            df = df_tickers[symbol_name]
            # Asks table
            asks = df[['Ask', 'AskQty']].rename(columns={'Ask': 'Price', 'AskQty': 'Quantity'}).sort_values('Price', ascending=False)

            st.dataframe(
                asks.style
                .apply(lambda _: ['color: red'] * asks.shape[1], axis=1)  # len = number of columns
                .format({'Price': '{:.2f}', 'Quantity': '{:.4f}'}),
                height=318
            )

            # Bids table
            bids = df[['Bid', 'BidQty']].rename(columns={'Bid': 'Price', 'BidQty': 'Quantity'}).sort_values('Price', ascending=False)

            st.dataframe(
                bids.style
                .apply(lambda _: ['color: green'] * bids.shape[1], axis=1)  # len = number of columns
                .format({'Price': '{:.2f}', 'Quantity': '{:.4f}'}),
                height=318
            )
    elif selected_tab == "Analysis":
        conn = st.connection("postgres", type="sql")
        
        # Create sub-tabs within Analysis
        analysis_tab1, analysis_tab2 = st.tabs(["Lead-Lag Analysis", "Test"])
        
        with analysis_tab1:
            st.title("🔄 Lead-Lag Cross-Correlation Analysis")
            st.markdown("""
            Analyze the cross-correlation between DEX trade prices and CEX price index at various lag intervals.
            This helps identify if DEX prices lead or lag behind CEX prices.
            """)
            
            # Time range selector
            col1, col2 = st.columns(2)
            with col1:
                time_range = st.selectbox(
                    "Time Range",
                    options=["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
                    index=2
                )
            
            # Convert time range to timedelta
            time_range_map = {
                "Last 1 Hour": timedelta(hours=1),
                "Last 6 Hours": timedelta(hours=6),
                "Last 24 Hours": timedelta(hours=24),
                "Last 7 Days": timedelta(days=7)
            }
            time_delta = time_range_map[time_range]
            end_time = datetime.utcnow()
            start_time = end_time - time_delta
            
            with col2:
                min_trades = st.number_input("Minimum Trades", min_value=5, max_value=1000, value=10, step=5)
            
            # Fetch available pools
            with st.spinner("Fetching available pools..."):
                pools_query = f"""
                    SELECT 
                        ds.pool_address,
                        ds.chain,
                        ds.dex,
                        t_in.symbol as token_in,
                        t_out.symbol as token_out,
                        COUNT(*) as trade_count
                    FROM dex_swaps ds
                    LEFT JOIN tokens t_in ON ds.token_in = t_in.address
                    LEFT JOIN tokens t_out ON ds.token_out = t_out.address
                    WHERE ds.time >= '{start_time.isoformat()}'::timestamptz 
                      AND ds.time <= '{end_time.isoformat()}'::timestamptz
                    GROUP BY ds.pool_address, ds.chain, ds.dex, t_in.symbol, t_out.symbol
                    HAVING COUNT(*) >= {min_trades}
                    ORDER BY trade_count DESC
                """
                pools_df = conn.query(pools_query, ttl=0)
            
            if pools_df.empty:
                st.warning("No pools found with sufficient trading activity in the selected time range.")
                st.info("Try selecting a longer time range or reducing the minimum trades threshold.")
            else:
                # Pool selector
                st.subheader("📊 Select Pool")
                
                # Create display names for pools
                pools_df['pair'] = pools_df['token_in'] + '/' + pools_df['token_out']
                pools_df['display_name'] = (
                    pools_df['pair'] + " (" + 
                    pools_df['dex'] + " on " + 
                    pools_df['chain'] + ") - " + 
                    pools_df['trade_count'].astype(str) + " trades"
                )
                
                selected_pool_idx = st.selectbox(
                    "Pool",
                    options=range(len(pools_df)),
                    format_func=lambda i: pools_df.iloc[i]['display_name']
                )
                
                selected_pool = pools_df.iloc[selected_pool_idx]
                pool_address = selected_pool['pool_address']
                pair = selected_pool['pair']
                
                # Display pool info
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Pair", pair)
                with col2:
                    st.metric("DEX", selected_pool['dex'])
                with col3:
                    st.metric("Chain", selected_pool['chain'])
                with col4:
                    st.metric("Trades", selected_pool['trade_count'])
                
                # CEX symbol input
                st.subheader("🔗 CEX Symbol Mapping")
                
                # Auto-detect CEX symbol
                default_cex_symbol = ""
                if 'USDT' in pair:
                    base = pair.split('/')[0]
                    default_cex_symbol = f"{base.lower()}usdt"
                elif 'USDC' in pair:
                    base = pair.split('/')[0]
                    default_cex_symbol = f"{base.lower()}usdt"
                
                cex_symbol = st.text_input(
                    "CEX Price Index Symbol",
                    value=default_cex_symbol,
                    help="Enter the symbol used in the price_index table (e.g., 'btcusdt', 'ethusdt')"
                )
                
                if cex_symbol and st.button("🚀 Run Lead-Lag Analysis", type="primary"):
                    with st.spinner(f"Analyzing {pair} on {selected_pool['dex']}..."):
                        try:
                            # Lag intervals in milliseconds
                            lag_intervals_ms = [0, 10, 30, 50, 100, 1000, 2000, 6000, 12000, 30000, 60000]
                            
                            # Fetch DEX trades
                            dex_query = f"""
                                SELECT time, price
                                FROM dex_swaps
                                WHERE pool_address = '{pool_address}'
                                  AND time >= '{start_time.isoformat()}'::timestamptz
                                  AND time <= '{end_time.isoformat()}'::timestamptz
                                ORDER BY time ASC
                            """
                            dex_trades = conn.query(dex_query, ttl=0)
                            
                            if dex_trades.empty:
                                st.error("No DEX trades found for this pool.")
                            else:
                                # Fetch CEX price index (extended backward for lags)
                                max_lag_seconds = max(lag_intervals_ms) / 1000
                                extended_start = start_time - timedelta(seconds=max_lag_seconds + 10)
                                
                                cex_query = f"""
                                    SELECT time, price_index
                                    FROM price_index
                                    WHERE symbol = '{cex_symbol.lower()}'
                                      AND time >= '{extended_start.isoformat()}'::timestamptz
                                      AND time <= '{end_time.isoformat()}'::timestamptz
                                    ORDER BY time ASC
                                """
                                cex_prices = conn.query(cex_query, ttl=0)
                                
                                if cex_prices.empty:
                                    st.error(f"No CEX price index found for {cex_symbol}")
                                else:
                                    # Calculate correlations for each lag
                                    results = []
                                    
                                    for lag_ms in lag_intervals_ms:
                                        lag_delta = timedelta(milliseconds=lag_ms)
                                        
                                        # Align prices
                                        dex_prices_list = []
                                        cex_prices_list = []
                                        
                                        cex_times = cex_prices['time'].values
                                        cex_vals = cex_prices['price_index'].values
                                        
                                        for _, dex_row in dex_trades.iterrows():
                                            dex_time = dex_row['time']
                                            dex_price = dex_row['price']
                                            
                                            # Target time in CEX data (lag backward)
                                            target_cex_time = dex_time - lag_delta
                                            
                                            # Find nearest CEX price
                                            time_diffs = np.abs((cex_times - target_cex_time).astype('timedelta64[ms]').astype(int))
                                            nearest_idx = np.argmin(time_diffs)
                                            
                                            # Only include if within 100ms tolerance
                                            if time_diffs[nearest_idx] <= 100:
                                                dex_prices_list.append(dex_price)
                                                cex_prices_list.append(cex_vals[nearest_idx])
                                        
                                        # Calculate correlation
                                        if len(dex_prices_list) >= 2:
                                            dex_arr = np.array(dex_prices_list)
                                            cex_arr = np.array(cex_prices_list)
                                            
                                            mask = np.isfinite(dex_arr) & np.isfinite(cex_arr)
                                            if np.sum(mask) >= 2:
                                                correlation = np.corrcoef(dex_arr[mask], cex_arr[mask])[0, 1]
                                            else:
                                                correlation = np.nan
                                        else:
                                            correlation = np.nan
                                        
                                        # Create label
                                        lag_label = f"{lag_ms}ms" if lag_ms < 1000 else f"{lag_ms // 1000}s"
                                        
                                        results.append({
                                            'lag_ms': lag_ms,
                                            'lag_label': lag_label,
                                            'correlation': correlation,
                                            'num_samples': len(dex_prices_list)
                                        })
                                    
                                    results_df = pd.DataFrame(results)
                                    
                                    # Display results
                                    st.success("✅ Analysis Complete!")
                                    
                                    # Summary statistics
                                    st.subheader("📈 Summary Statistics")
                                    
                                    valid_results = results_df[results_df['correlation'].notna()]
                                    if not valid_results.empty:
                                        max_corr_idx = valid_results['correlation'].idxmax()
                                        max_corr_row = valid_results.loc[max_corr_idx]
                                        
                                        col1, col2, col3, col4 = st.columns(4)
                                        with col1:
                                            st.metric("Max Correlation", f"{max_corr_row['correlation']:.4f}")
                                        with col2:
                                            st.metric("Optimal Lag", max_corr_row['lag_label'])
                                        with col3:
                                            avg_corr = valid_results['correlation'].mean()
                                            st.metric("Avg Correlation", f"{avg_corr:.4f}")
                                        with col4:
                                            avg_samples = valid_results['num_samples'].mean()
                                            st.metric("Avg Samples", f"{int(avg_samples)}")
                                        
                                        # Interpretation
                                        if max_corr_row['lag_ms'] == 0:
                                            interpretation = "🟢 DEX and CEX prices are synchronized (no significant lead/lag)"
                                        elif max_corr_row['lag_ms'] > 0:
                                            interpretation = f"🔵 CEX leads DEX by ~{max_corr_row['lag_label']}"
                                        
                                        st.info(f"**Interpretation:** {interpretation}")
                                    
                                    # Visualizations
                                    st.subheader("📊 Correlation vs Lag")
                                    
                                    # Line plot
                                    fig_line = go.Figure()
                                    
                                    fig_line.add_trace(go.Scatter(
                                        x=results_df['lag_ms'],
                                        y=results_df['correlation'],
                                        mode='lines+markers',
                                        name='Correlation',
                                        line=dict(color='#1f77b4', width=3),
                                        marker=dict(size=8)
                                    ))
                                    
                                    # Highlight maximum
                                    if not valid_results.empty:
                                        fig_line.add_trace(go.Scatter(
                                            x=[max_corr_row['lag_ms']],
                                            y=[max_corr_row['correlation']],
                                            mode='markers',
                                            name='Optimal Lag',
                                            marker=dict(size=15, color='red', symbol='star')
                                        ))
                                    
                                    fig_line.update_layout(
                                        title=f"Cross-Correlation: {pair} ({selected_pool['dex']})",
                                        xaxis_title="Lag (ms)",
                                        yaxis_title="Correlation Coefficient",
                                        yaxis_range=[-1, 1],
                                        hovermode='x unified',
                                        height=500
                                    )
                                    
                                    st.plotly_chart(fig_line, use_container_width=True)
                                    
                                    # Bar chart
                                    fig_bar = px.bar(
                                        results_df,
                                        x='lag_label',
                                        y='correlation',
                                        title="Correlation by Lag Interval",
                                        labels={'lag_label': 'Lag', 'correlation': 'Correlation'},
                                        color='correlation',
                                        color_continuous_scale='RdYlGn',
                                        range_color=[-1, 1]
                                    )
                                    
                                    fig_bar.update_layout(height=400)
                                    st.plotly_chart(fig_bar, use_container_width=True)
                                    
                                    # Data table
                                    st.subheader("📋 Detailed Results")
                                    st.dataframe(results_df, use_container_width=True, hide_index=True)
                                    
                        except Exception as e:
                            st.error(f"Analysis failed: {e}")
                            import traceback
                            st.code(traceback.format_exc())
        
        with analysis_tab2:
            st.title("🧪 Test Tab")
            st.write("This is a test tab for development purposes.")
            
            # Simple test query
            if st.button("Run Test Query"):
                df = conn.query("""
                    SELECT
                        time_bucket('60 minute', time) AS candle_time,
                        first(price_index, time) AS open,
                        max(price_index)         AS high,
                        min(price_index)         AS low,
                        last(price_index, time)  AS close
                    FROM price_index
                    WHERE symbol = 'btcusdt'
                    GROUP BY candle_time
                    ORDER BY candle_time
                    LIMIT 100;
                """)
                st.dataframe(df)


if __name__ == "__main__":
    shm_path = os.getenv("SHM_PATH", ".price_ix.data")
    shm_data = read_shm(shm_path)
    tickers_data = shm_data["tickers"]
    price_indices_data = shm_data["price_indices"]

    df_price_index = get_price_indices_df(price_indices_data)
    df_tickers = get_ticker_dfs(tickers_data)
    main()
