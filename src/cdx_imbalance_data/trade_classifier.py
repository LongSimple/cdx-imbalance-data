import logging
import pandas as pd
import datetime

from . import config
from . import economic_conversions as econ_conv

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def classify_trades(
    dtcc_trades_df: pd.DataFrame, bloomberg_mids_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Classifies DTCC trades as 'bid_side', 'offer_side', 'mid_market', or 'unclassifiable'
    by comparing them to interpolated Bloomberg mid-prices/spreads.

    Args:
        dtcc_trades_df: DataFrame of preprocessed DTCC trades.
                        Expected columns include 'execution_timestamp_utc', 'product_name',
                        'price_value', 'price_notation', 'spread_value', 'spread_notation'.
        bloomberg_mids_df: DataFrame of Bloomberg mid-prices.
                           Expected columns: 'timestamp_utc', 'mid_price' (which is a spread).

    Returns:
        The DTCC trades DataFrame with an added 'trade_classification' column.
    """
    if dtcc_trades_df.empty:
        logger.warning("DTCC trades DataFrame is empty. No trades to classify.")
        return dtcc_trades_df.copy() # Return a copy to avoid modifying original if it was empty
    
    if bloomberg_mids_df.empty:
        logger.warning(
            "Bloomberg mids DataFrame is empty. All trades will be 'unclassifiable_no_mid'."
        )
        dtcc_trades_df["trade_classification"] = "unclassifiable_no_mid"
        return dtcc_trades_df

    # Ensure Bloomberg data is sorted by time for interpolation
    bloomberg_mids_df = bloomberg_mids_df.sort_values(
        by="timestamp_utc"
    ).set_index("timestamp_utc")

    classifications = []

    for _, trade_row in dtcc_trades_df.iterrows():
        trade_ts = trade_row["execution_timestamp_utc"]
        classification = "unclassifiable_error" # Default

        # 1. Normalize DTCC quote (determine basis and value)
        dtcc_quote_basis, dtcc_quote_value, _ = econ_conv.normalize_dtcc_quote(trade_row)

        if dtcc_quote_basis is None or pd.isna(dtcc_quote_value):
            classification = "unclassifiable_bad_dtcc_quote"
            classifications.append(classification)
            continue

        # 2. Find relevant Bloomberg mid
        # Define the window for interpolation based on latency filter
        time_window_start = trade_ts - datetime.timedelta(
            seconds=config.LATENCY_FILTER_SECONDS
        )
        time_window_end = trade_ts + datetime.timedelta(
            seconds=config.LATENCY_FILTER_SECONDS
        )

        # Get Bloomberg mids within the latency window around the trade
        relevant_bberg_mids = bloomberg_mids_df[
            (bloomberg_mids_df.index >= time_window_start) &
            (bloomberg_mids_df.index <= time_window_end)
        ]

        if relevant_bberg_mids.empty:
            classification = "unclassifiable_stale_mid" # No mid within latency window
            classifications.append(classification)
            continue
        
        # Interpolate: find the closest mid if multiple, or use as-of merge
        # For simplicity, using pandas merge_asof to get the latest mid before or at trade_ts
        # We need to ensure bloomberg_mids_df is suitable for merge_asof (sorted index)
        # And dtcc_trades_df needs a temporary column for merge_asof if its index isn't timestamp
        
        # Alternative: find closest mid_price in `relevant_bberg_mids`
        # Calculate time difference to all relevant mids
        relevant_bberg_mids["time_diff"] = abs(relevant_bberg_mids.index - trade_ts)
        closest_mid_row = relevant_bberg_mids.loc[relevant_bberg_mids["time_diff"].idxmin()]
        
        bberg_mid_spread = closest_mid_row["mid_price"] # This is a spread from PX_MID

        if pd.isna(bberg_mid_spread):
            classification = "unclassifiable_nan_bberg_mid"
            classifications.append(classification)
            continue

        # 3. Economic Comparison
        try:
            if dtcc_quote_basis == "spread":
                # Both are spreads (DTCC spread vs Bloomberg mid-spread)
                # Ensure units are consistent (assume BPS for both for now)
                dtcc_s = float(dtcc_quote_value)
                bberg_s = float(bberg_mid_spread)

                if dtcc_s < bberg_s:
                    classification = "bid_side"
                elif dtcc_s > bberg_s:
                    classification = "offer_side"
                else:
                    classification = "mid_market"

            elif dtcc_quote_basis == "price":
                # DTCC is price, Bloomberg mid is spread. Convert Bloomberg mid-spread to price.
                # This requires the economic_conversions.convert_spread_to_price
                # and parameters like tenor, coupon, recovery.
                # For now, using placeholder.
                # TODO: Get actual tenor for the specific product if possible, default to 5Y
                bberg_mid_price_converted = econ_conv.convert_spread_to_price(
                    spread_bps=float(bberg_mid_spread)
                    # Add other params like years_to_maturity if available from product_name
                )

                if pd.isna(bberg_mid_price_converted):
                    classification = "unclassifiable_conversion_failed"
                else:
                    dtcc_p = float(dtcc_quote_value)
                    if dtcc_p < bberg_mid_price_converted:
                        classification = "bid_side" # Assuming lower price means bid for upfront points
                    elif dtcc_p > bberg_mid_price_converted:
                        classification = "offer_side" # Assuming higher price means offer
                    else:
                        classification = "mid_market"
            else:
                classification = "unclassifiable_unknown_basis"
        
        except ValueError:
            logger.error(f"ValueError during comparison for trade {trade_row.get('dissemination_id')}")
            classification = "unclassifiable_comparison_error"
        except Exception as e:
            logger.error(f"Unexpected error during comparison for trade {trade_row.get('dissemination_id')}: {e}")
            classification = "unclassifiable_comparison_error"
            
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
    classified_df = classify_trades(dtcc_df.copy(), bberg_df.copy()) # Use copies
    
    if classified_df is not None:
        logger.info("Classification Results:")
        print(classified_df[["dissemination_id", "trade_classification", "price_value", "spread_value"]])
    else:
        logger.error("Classification failed.")

    # Example with empty bloomberg data
    logger.info("\nClassifying with empty Bloomberg data...")
    classified_empty_bberg_df = classify_trades(dtcc_df.copy(), pd.DataFrame(columns=bberg_df.columns))
    if classified_empty_bberg_df is not None:
        print(classified_empty_bberg_df[["dissemination_id", "trade_classification"]])

    # Example with empty dtcc data
    logger.info("\nClassifying with empty DTCC data...")
    classified_empty_dtcc_df = classify_trades(pd.DataFrame(columns=dtcc_df.columns), bberg_df.copy())
    if classified_empty_dtcc_df is not None:
        print(classified_empty_dtcc_df)
