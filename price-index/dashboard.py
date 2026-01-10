import pandas as pd
import streamlit as st
from ffi import get_ticker_dfs,get_price_indices_df, Symbols, read_shm
from streamlit_autorefresh import st_autorefresh

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
        st.title("Data Analysis")
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
                        LIMIT 1000;
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
