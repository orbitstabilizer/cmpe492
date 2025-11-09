import streamlit as st
from ffi import load_data, get_symol_df, Symbols
from streamlit_autorefresh import st_autorefresh

shm_path = ".ticker.data"
data = load_data(shm_path)


    

def main():
    refresh_interval = st.sidebar.number_input(
        "Auto-Refresh Interval (seconds/10)", min_value=5, max_value=600, value=10, step=1
    ) * 100  # Convert to milliseconds
    st_autorefresh(interval=refresh_interval, limit=None)

    st.title("Dashboard")

    # choose symbol index
    symbol_index = st.sidebar.selectbox("Select Symbol Index", options=[0, 1, 2], index=0)
    st.subheader(f"{Symbols(symbol_index).name}")
    df = get_symol_df(data, symbol_index)
    st.dataframe(df)


if __name__ == "__main__":
    main()
