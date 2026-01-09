import streamlit as st
from ffi import get_ticker_dfs,get_price_indices_df, Symbols, read_shm
from streamlit_autorefresh import st_autorefresh

shm_path = ".price_ix.data"
shm_data = read_shm(shm_path)
tickers_data = shm_data["tickers"]
price_indices_data = shm_data["price_indices"]

df_price_index = get_price_indices_df(price_indices_data)
df_tickers = get_ticker_dfs(tickers_data)


def main():
    refresh_interval = st.sidebar.number_input(
        "Auto-Refresh Interval (seconds/10)", min_value=1, max_value=600, value=1, step=1
    ) * 100  # Convert to milliseconds
    st.markdown("""
        <style>
        stStatusWidget{display:none!important;}
        </style>
    """,unsafe_allow_html=True)
    st_autorefresh(interval=refresh_interval, limit=None)

    st.title("Price Index")
    tab_detatiled, tab_price_index = st.tabs(["Detailed View", "Price Index"])
    with tab_detatiled:
        st.title("Detailed View")
        # choose symbol index
        symbol_name = st.selectbox("Select Symbol Index", options=[sym.name for sym in Symbols], index=0)
        price_index = float(df_price_index.loc[symbol_name].iloc[0])
        st.subheader(f"{symbol_name}: Price Index: {price_index:.6f}")
        df_ticker = df_tickers[symbol_name]
        st.dataframe(df_ticker)

    with tab_price_index:
        st.title("Price Index Summary")
        st.dataframe(df_price_index, height=1000)

if __name__ == "__main__":
    main()
