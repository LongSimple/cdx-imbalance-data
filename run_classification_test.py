import logging
import datetime as dt
import sys
import os
import json
import pandas as pd
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from cdx_imbalance_data import config
from cdx_imbalance_data import download_and_parse_eod_cumulative_file, preprocess_dtcc_trades
from cdx_imbalance_data import BloombergConnector
from cdx_imbalance_data import classify_trades

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
# Suppress the font finding logs from matplotlib
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def plot_classified_trades(classified_df: pd.DataFrame, bloomberg_ticks_df: pd.DataFrame, security_name: str):
    """
    Generates and saves a plot of classified trades against Bloomberg bid, ask, and mid-prices.
    """
    if classified_df.empty and bloomberg_ticks_df.empty:
        logger.warning("Cannot plot empty DataFrames.")
        return

    plt.figure(figsize=(17, 8))

    # Prepare Bloomberg data for plotting
    ticks = bloomberg_ticks_df.copy()
    # Convert to EST for plotting
    ticks['timestamp_est'] = pd.to_datetime(ticks['timestamp']).dt.tz_convert('America/New_York')
    ticks = ticks.sort_values(by="timestamp_est").set_index("timestamp_est")
    
    bids = ticks[ticks['typ'] == 'BID']['value'].rename('bid')
    asks = ticks[ticks['typ'] == 'ASK']['value'].rename('ask')
    
    # Plot Bid and Ask prices
    bid_line, = plt.plot(bids.index, bids, color='darkred', linestyle='--', alpha=0.7, label='Bloomberg Bid', visible=False)
    ask_line, = plt.plot(asks.index, asks, color='darkgreen', linestyle='--', alpha=0.7, label='Bloomberg Ask', visible=False)

    # Calculate and plot mid-price
    market_state = pd.concat([bids, asks], axis=1).ffill()
    market_state['mid'] = (market_state['bid'] + market_state['ask']) / 2.0
    mid_line, = plt.plot(market_state.index, market_state['mid'], color='blue', linestyle='-', label='Bloomberg Mid-Price')

    # Overlay classified trades
    if not classified_df.empty:
        # Convert spread values to numeric for plotting and timestamps to EST
        classified_df['spread_value_numeric'] = pd.to_numeric(classified_df['spread_value'], errors='coerce') * 10000
        classified_df['execution_timestamp_est'] = pd.to_datetime(classified_df['execution_timestamp_utc']).dt.tz_convert('America/New_York')
        
        buy_trades = classified_df[classified_df['trade_classification'] == 'buy']
        sell_trades = classified_df[classified_df['trade_classification'] == 'sell']
        mid_trades = classified_df[classified_df['trade_classification'] == 'mid_market']

        plt.scatter(buy_trades['execution_timestamp_est'], buy_trades['spread_value_numeric'], 
                    color='lime', marker='^', s=120, edgecolors='black', label='Buy (DTCC)', zorder=5)
        plt.scatter(sell_trades['execution_timestamp_est'], sell_trades['spread_value_numeric'], 
                    color='red', marker='v', s=120, edgecolors='black', label='Sell (DTCC)', zorder=5)
        plt.scatter(mid_trades['execution_timestamp_est'], mid_trades['spread_value_numeric'],
                    color='yellow', marker='o', s=100, edgecolors='black', label='Mid-Market (DTCC)', zorder=4)

    plt.title(f"Trade Classifications vs. Bloomberg Market for {security_name}", fontsize=16)
    plt.xlabel("Execution Time (EST)", fontsize=12)
    plt.ylabel("Spread (BPS)", fontsize=12)
    leg = plt.legend()
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)

    # Make the legend interactive
    leg_lines = leg.get_lines()
    leg_texts = leg.get_texts()
    lined = {}
    for legline, legtext, origline in zip(leg_lines, leg_texts, [bid_line, ask_line, mid_line]):
        legline.set_picker(True)
        lined[legline] = origline
        if not origline.get_visible():
            legline.set_alpha(0.2)
            legtext.set_alpha(0.2)

    def on_pick(event):
        legline = event.artist
        origline = lined[legline]
        visible = not origline.get_visible()
        origline.set_visible(visible)
        legline.set_alpha(1.0 if visible else 0.2)
        for text in leg.get_texts():
            if legline.get_label() == text.get_text():
                text.set_alpha(1.0 if visible else 0.2)
        plt.gcf().canvas.draw()

    plt.gcf().canvas.mpl_connect('pick_event', on_pick)
    
    # Format the x-axis to show time more clearly
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(interval=1))
    plt.gcf().autofmt_xdate() # Auto-rotates the labels to prevent overlap

    plt.tight_layout()
    
    output_path = "classified_trades.png"
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")
    plt.close()


def run_test():
    """
    Fetches DTCC and Bloomberg data for a specific day, classifies trades,
    and saves the result to a Parquet file.
    """
    test_date = dt.date(2025, 6, 16)
    asset_class = "CREDITS"
    
    # 1. Fetch DTCC EOD data
    logger.info(f"Fetching DTCC EOD data for {test_date.isoformat()}...")
    dtcc_df = download_and_parse_eod_cumulative_file(asset_class, test_date)
    
    if dtcc_df is None or dtcc_df.empty:
        logger.error("Failed to fetch or parse DTCC EOD data. Aborting test.")
        return
    logger.info("Raw DTCC Data:")
    print(dtcc_df.head())

    # 2. Preprocess the DTCC data
    logger.info("Preprocessing DTCC data...")
    preprocessed_dtcc_df = preprocess_dtcc_trades(dtcc_df, source_type="EOD_CUMULATIVE")

    if preprocessed_dtcc_df is None or preprocessed_dtcc_df.empty:
        logger.error("No trades remaining after preprocessing. Aborting test.")
        return
    logger.info("Preprocessed DTCC Data:")
    print(preprocessed_dtcc_df.head())

    # 3. Fetch Bloomberg tick data
    with open('data/upi_ticker_map.json', 'r') as f:
        upi_map = json.load(f)
    
    target_info = upi_map.get(config.TARGET_UPI)
    if not target_info:
        logger.error(f"TARGET_UPI '{config.TARGET_UPI}' not found in upi_ticker_map.json. Aborting.")
        return

    target_ticker = target_info.get("ticker")
    notation_type = target_info.get("notation")

    if not target_ticker or not notation_type:
        logger.error(f"Incomplete information for TARGET_UPI '{config.TARGET_UPI}' in upi_ticker_map.json. Aborting.")
        return

    logger.info(f"Fetching Bloomberg tick data for {target_ticker} (UPI: {config.TARGET_UPI}, Notation: {notation_type}) on {test_date.isoformat()}...")
    bbg_connector = BloombergConnector()
    try:
        bloomberg_ticks_df = bbg_connector.get_tick_data(target_ticker, test_date)
        if bloomberg_ticks_df is None or bloomberg_ticks_df.empty:
            logger.error("No Bloomberg tick data returned. Aborting test.")
            return
        logger.info("Raw Bloomberg Tick Data:")
        print(bloomberg_ticks_df.head())
    except Exception as e:
        logger.error(f"Failed to fetch Bloomberg data: {e}", exc_info=True)
        logger.error("Please ensure the Bloomberg terminal is running and you are logged in.")
        return
    finally:
        bbg_connector.stop()


    # 4. Classify trades
    logger.info("Classifying trades...")
    classified_df = classify_trades(preprocessed_dtcc_df, bloomberg_ticks_df, notation_type)

    if classified_df.empty:
        logger.warning("No trades were classified.")
    else:
        logger.info("Classification complete. Results:")
        print(classified_df.head())

        # 5. Save to Parquet
        output_path = "classified_trades_test_run.parquet"
        classified_df.to_parquet(output_path)
        logger.info(f"Successfully saved classified trades to {output_path}")

        # 6. Plot the results
        plot_classified_trades(classified_df, bloomberg_ticks_df, target_ticker)


if __name__ == "__main__":
    run_test()
