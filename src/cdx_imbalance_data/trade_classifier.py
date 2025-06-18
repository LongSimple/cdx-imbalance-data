import logging
import pandas as pd
import datetime as dt

from . import config
from . import economic_conversions as econ_conv

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _create_mid_price_series(bloomberg_ticks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a continuous mid-price series from raw Bloomberg bid/ask ticks.
    """
    ticks = bloomberg_ticks_df.copy()
    # Timestamps from bloomberg_connector are already timezone-aware ('America/New_York')
    # Convert to UTC for comparison with DTCC data
    ticks['timestamp_utc'] = pd.to_datetime(ticks['timestamp']).dt.tz_convert('UTC')
    ticks = ticks.sort_values(by="timestamp_utc").set_index("timestamp_utc")
    
    # Separate bids and asks
    bids = ticks[ticks['typ'] == 'BID'][['value']].rename(columns={'value': 'bid_price'})
    asks = ticks[ticks['typ'] == 'ASK'][['value']].rename(columns={'value': 'ask_price'})

    # Combine and forward-fill
    market_state = pd.concat([bids, asks], axis=1).ffill()
    
    # Calculate mid-price
    market_state['mid_price'] = (market_state['bid_price'] + market_state['ask_price']) / 2.0
    
    # Drop rows where mid-price is still NaN (i.e., before the first bid/ask pair)
    market_state.dropna(subset=['mid_price'], inplace=True)
    
    logger.info(f"Created mid-price series with {len(market_state)} entries.")
    logger.info("Mid-price time series:")
    print(market_state.head())
    return market_state


def classify_trades(
    dtcc_trades_df: pd.DataFrame, bloomberg_ticks_df: pd.DataFrame, notation_type: str
) -> pd.DataFrame:
    """
    Classifies DTCC trades by comparing them to the calculated Bloomberg mid-price.
    """
    if dtcc_trades_df.empty:
        logger.warning("DTCC trades DataFrame is empty. No trades to classify.")
        return dtcc_trades_df.copy()

    # Filter for the target UPI and remove package trades
    if 'unique_product_identifier' in dtcc_trades_df.columns and config.TARGET_UPI:
        dtcc_trades_df = dtcc_trades_df[dtcc_trades_df['unique_product_identifier'] == config.TARGET_UPI].copy()
        logger.info(f"Filtered for TARGET_UPI '{config.TARGET_UPI}'. {len(dtcc_trades_df)} trades remaining.")
    if 'package_indicator' in dtcc_trades_df.columns:
        dtcc_trades_df = dtcc_trades_df[dtcc_trades_df['package_indicator'] != 'Y'].copy()
        logger.info(f"Filtered out package trades. {len(dtcc_trades_df)} trades remaining.")

    if dtcc_trades_df.empty:
        logger.warning("No trades remaining after filtering.")
        return dtcc_trades_df

    if bloomberg_ticks_df.empty:
        logger.warning("Bloomberg ticks DataFrame is empty. All trades will be 'unclassifiable_no_mid'.")
        dtcc_trades_df["trade_classification"] = "unclassifiable_no_mid"
        return dtcc_trades_df

    # Create the continuous mid-price series
    bberg_mid_series = _create_mid_price_series(bloomberg_ticks_df)

    if bberg_mid_series.empty:
        logger.warning("Could not create mid-price series from Bloomberg data. All trades will be 'unclassifiable_no_mid'.")
        dtcc_trades_df["trade_classification"] = "unclassifiable_no_mid"
        return dtcc_trades_df

    classifications = []
    for _, trade_row in dtcc_trades_df.iterrows():
        trade_ts = trade_row["execution_timestamp_utc"]
        classification = "unclassifiable_error"

        # 1. Normalize DTCC quote
        dtcc_quote_basis, dtcc_quote_value, _ = econ_conv.normalize_dtcc_quote(trade_row)
        if dtcc_quote_basis is None or pd.isna(dtcc_quote_value):
            classification = "unclassifiable_bad_dtcc_quote"
            classifications.append(classification)
            continue

        # 2. Find relevant Bloomberg mid-price
        try:
            closest_market_state = bberg_mid_series.asof(trade_ts)
            if pd.isna(closest_market_state['mid_price']):
                classification = "unclassifiable_stale_mid"
                classifications.append(classification)
                continue
            
            time_to_mid = abs(trade_ts - closest_market_state.name)
            if time_to_mid > dt.timedelta(seconds=config.LATENCY_FILTER_SECONDS):
                classification = "unclassifiable_stale_mid"
                classifications.append(classification)
                continue
            
            bberg_mid_spread = closest_market_state["mid_price"]

            # 3. Economic Comparison
            trade_spread_bps = None
            if notation_type == "spread":
                trade_spread_bps = float(dtcc_quote_value) * 10000
            elif notation_type == "price":
                trade_spread_bps = econ_conv.convert_price_to_spread(price=float(dtcc_quote_value))

            if trade_spread_bps is None or pd.isna(trade_spread_bps):
                classification = "unclassifiable_conversion_failed"
            else:
                if trade_spread_bps < 0:
                    logger.warning(f"Filtering out trade with negative spread: {trade_spread_bps} bps")
                    classification = "unclassifiable_negative_spread"
                elif trade_spread_bps > bberg_mid_spread:
                    classification = "buy"
                elif trade_spread_bps < bberg_mid_spread:
                    classification = "sell"
                else:
                    classification = "mid_market"
        
        except (ValueError, TypeError) as e:
            logger.error(f"Error during comparison for trade {trade_row.get('dissemination_id')}: {e}")
            classification = "unclassifiable_comparison_error"
        
        logger.info(
            f"Trade {trade_row.get('dissemination_id')}: "
            f"DTCC Quote ({notation_type}) = {trade_spread_bps} @ {trade_ts}, "
            f"Bloomberg Mid = {bberg_mid_spread} @ {closest_market_state.name}, "
            f"Classification = {classification}"
        )
        classifications.append(classification)

    dtcc_trades_df["trade_classification"] = classifications
    return dtcc_trades_df


if __name__ == "__main__":
    logger.info("Trade Classifier Module - Example Usage")

    # Create dummy DTCC data
    dummy_dtcc_data = {
        "execution_timestamp_utc": [
            pd.Timestamp("2023-01-01 10:00:00", tz="UTC"),
            pd.Timestamp("2023-01-01 10:05:00", tz="UTC"),
            pd.Timestamp("2023-01-01 10:10:00", tz="UTC"),
        ],
        "dissemination_id": ["D001", "D002", "D003"],
        "product_name": ["CDX.NA.IG.S40.5Y", "CDX.NA.IG.S40.5Y", "CDX.NA.IG.S40.5Y"],
        "price_value": [None, "1.5", None], # D002 is price based
        "price_notation": [None, "POINTS_UPFRONT", None],
        "spread_value": ["70.0", None, "72.0"], # D001, D003 are spread based
        "spread_notation": ["BPS", None, "BPS"],
        "notional_amount": [10e6, 5e6, 15e6]
    }
    dtcc_df = pd.DataFrame(dummy_dtcc_data)

    # Create dummy Bloomberg mid data (spreads)
    dummy_bberg_data = {
        "timestamp_utc": [
            pd.Timestamp("2023-01-01 09:59:50", tz="UTC"), # Before D001
            pd.Timestamp("2023-01-01 10:00:05", tz="UTC"), # Near D001
            pd.Timestamp("2023-01-01 10:04:50", tz="UTC"), # Before D002
            pd.Timestamp("2023-01-01 10:05:03", tz="UTC"), # Near D002
            pd.Timestamp("2023-01-01 10:09:55", tz="UTC"), # Near D003
            pd.Timestamp("2023-01-01 10:10:10", tz="UTC"), # After D003
        ],
        "mid_price": [70.5, 70.6, 70.0, 69.9, 71.5, 71.6],  # These are spreads
    }
    bberg_df = pd.DataFrame(dummy_bberg_data)

    logger.info("Classifying dummy trades...")
    classified_df = classify_trades(dtcc_df.copy(), bberg_df.copy(), "spread") # Use copies
    
    if classified_df is not None:
        logger.info("Classification Results:")
        print(classified_df[["dissemination_id", "trade_classification", "price_value", "spread_value"]])
    else:
        logger.error("Classification failed.")

    # Example with empty bloomberg data
    logger.info("\nClassifying with empty Bloomberg data...")
    classified_empty_bberg_df = classify_trades(dtcc_df.copy(), pd.DataFrame(columns=bberg_df.columns), "spread")
    if classified_empty_bberg_df is not None:
        print(classified_empty_bberg_df[["dissemination_id", "trade_classification"]])

    # Example with empty dtcc data
    logger.info("\nClassifying with empty DTCC data...")
    classified_empty_dtcc_df = classify_trades(pd.DataFrame(columns=dtcc_df.columns), bberg_df.copy(), "spread")
    if classified_empty_dtcc_df is not None:
        print(classified_empty_dtcc_df)
