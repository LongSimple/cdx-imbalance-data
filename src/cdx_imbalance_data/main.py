import logging
import os
import pandas as pd

from . import config
from . import dtcc_fetcher
from . import bloomberg_connector
from . import trade_classifier
# economic_conversions is used by trade_classifier

# Configure logging
# BasicConfig should be called only once, preferably at the application entry point.
# If other modules also call it, the first one takes precedence or behavior can be unexpected.
# For simplicity here, assuming this is the main entry point where logging is set up.
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler() # Ensure logs go to console
        # TODO: Add FileHandler if logs need to be saved to a file
    ]
)
logger = logging.getLogger(__name__) # Get logger for this module

# def run_classification_pipeline():
#     """
#     Orchestrates the entire pipeline:
#     1. Fetches and preprocesses DTCC data.
#     2. Fetches corresponding Bloomberg mid-prices.
#     3. Classifies trades.
#     4. Saves the results to a CSV file.
#     """
#     logger.info("Starting CDX imbalance data classification pipeline...")

#     # --- 1. Fetch and preprocess DTCC data ---
#     logger.info("Fetching DTCC data...")
#     # dtcc_trades_df = dtcc_fetcher.get_dtcc_data() # Old function, removed

#     # if dtcc_trades_df is None or dtcc_trades_df.empty:
#     #     logger.error("Failed to retrieve or process DTCC data. Pipeline cannot continue.")
#     #     return

#     # logger.info(f"Successfully processed {len(dtcc_trades_df)} DTCC trades.")

#     # # Determine the trade date from DTCC data to fetch corresponding Bloomberg data
#     # # Assuming all trades in a single DTCC file are for the same effective date,
#     # # or we take the date of the first trade.
#     # # A more robust way would be to determine the date from the DTCC filename if possible,
#     # # or ensure all trades in the processed df are for a single, consistent date.
#     # if not dtcc_trades_df["execution_timestamp_utc"].empty:
#     #     # Get the date from the first trade's execution timestamp
#     #     # Ensure it's converted to a date object
#     #     trade_date_for_bberg = dtcc_trades_df["execution_timestamp_utc"].iloc[0].date()
#     #     logger.info(f"Determined trade date for Bloomberg data: {trade_date_for_bberg}")
#     # else:
#     #     logger.error("No execution timestamps in DTCC data to determine trade date for Bloomberg. Aborting.")
#     #     return

#     # # --- 2. Fetch Bloomberg mid-prices ---
#     # logger.info(f"Fetching Bloomberg mid-prices for ticker {config.BLOOMBERG_CDX_IG_5Y_TICKER} on {trade_date_for_bberg}...")
#     # bloomberg_mids_df = bloomberg_connector.get_bloomberg_mid_prices(
#     #     ticker=config.BLOOMBERG_CDX_IG_5Y_TICKER,
#     #     trade_date=trade_date_for_bberg
#     # )

#     # if bloomberg_mids_df is None:
#     #     logger.warning(
#     #         "Failed to retrieve Bloomberg mid-prices. "
#     #         "Trades will be classified without Bloomberg data if possible, or marked unclassifiable."
#     #     )
#     #     # Create an empty DataFrame with expected columns to prevent downstream errors
#     #     bloomberg_mids_df = pd.DataFrame(columns=['timestamp_utc', 'mid_price'])


#     # if bloomberg_mids_df.empty:
#     #      logger.warning("Bloomberg mid-prices DataFrame is empty.")
#     # else:
#     #     logger.info(f"Successfully fetched {len(bloomberg_mids_df)} Bloomberg mid-price ticks.")


#     # # --- 3. Classify Trades ---
#     # logger.info("Classifying trades...")
#     # classified_trades_df = trade_classifier.classify_trades(
#     #     dtcc_trades_df.copy(), # Pass a copy to avoid modifying original
#     #     bloomberg_mids_df.copy() # Pass a copy
#     # )

#     # if classified_trades_df.empty:
#     #     logger.warning("No trades were classified. Output will be empty.")
#     # else:
#     #     logger.info(f"Successfully classified {len(classified_trades_df)} trades.")

#     # # --- 4. Save Results ---
#     # os.makedirs(config.OUTPUT_CSV_DIR, exist_ok=True)
    
#     # # Generate a filename with the date
#     # # Using the date derived for Bloomberg query, which should match DTCC data date
#     # output_filename = f"classified_cdx_ig_trades_{trade_date_for_bberg.strftime('%Y%m%d')}.csv"
#     # output_filepath = os.path.join(config.OUTPUT_CSV_DIR, output_filename)

#     # try:
#     #     classified_trades_df.to_csv(output_filepath, index=False)
#     #     logger.info(f"Successfully saved classified trades to: {output_filepath}")
#     # except Exception as e:
#     #     logger.error(f"Failed to save classified trades to {output_filepath}: {e}")

#     logger.info("CDX imbalance data classification pipeline finished.")


if __name__ == "__main__":
    # This will run the full pipeline.
    # Ensure all configurations in config.py are correct.
    # Ensure Bloomberg Terminal is running and xbbg/blpapi are set up if not mocking.
    # run_classification_pipeline() # Old pipeline is commented out
    logger.info("Main.py execution complete (run_classification_pipeline commented out).")
    logger.info("To run the DTCC poller, execute dtcc_fetcher.py directly (e.g., python -m src.cdx_imbalance_data.dtcc_fetcher).")
    pass # Placeholder if no other main logic is present
