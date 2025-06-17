import datetime as dt
import logging
import pandas as pd
import cloudscraper # For PPD API Slices
import time # For potential delays
# import pathlib # For saving files, if uncommented (Pathlib removed as it's currently unused)

# Assuming data_aggregator.py is in the same directory as dtcc_fetcher.py and config.py
# Adjust imports if necessary based on how the script is run (e.g., as a module)
from . import config
from . import dtcc_fetcher

# Configure logging (consistent with dtcc_fetcher.py)
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def aggregate_data_for_day(target_date: dt.date, asset_class: str) -> pd.DataFrame | None:
    """
    Fetches, preprocesses, and aggregates DTCC data for a specific day and asset class.
    """
    logger.info(f"Starting data aggregation for {asset_class} on {target_date.isoformat()}")
    all_dataframes = []

    # Initialize CloudScraper for PPD API Slices
    # Ensure config.CS_BROWSER_VERSION and config.CS_REFERER_URL are set in config.py
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True, "version": getattr(config, 'CS_BROWSER_VERSION', '110.0.0.0')}
    )
    scraper.headers.update({"Referer": getattr(config, 'CS_REFERER_URL', 'https://pddata.dtcc.com/')})
    logger.info(f"Cloudscraper initialized with browser version: {getattr(config, 'CS_BROWSER_VERSION', '110.0.0.0')}")

    # 1. Attempt to fetch EOD Cumulative data
    # Using config.FETCH_EOD_CUMULATIVE_FOR_LOOKBACK as a proxy, or add a specific config for aggregator
    if getattr(config, 'FETCH_EOD_CUMULATIVE_FOR_LOOKBACK', True):
        logger.info(f"Attempting to fetch S3 EOD CUMULATIVE for {asset_class} on {target_date.isoformat()}")
        eod_df_raw = dtcc_fetcher.download_and_parse_eod_cumulative_file(asset_class, target_date)
        if eod_df_raw is not None and not eod_df_raw.empty:
            logger.info(f"Successfully fetched S3 EOD CUMULATIVE. Raw shape: {eod_df_raw.shape}") # Changed log
            eod_df_after_fetcher_preprocessing = dtcc_fetcher.preprocess_dtcc_trades(eod_df_raw.copy(), source_type="EOD_CUMULATIVE")
            
            if eod_df_after_fetcher_preprocessing is not None and not eod_df_after_fetcher_preprocessing.empty:
                logger.info(f"S3 EOD CUMULATIVE data after fetcher's preprocessing (before aggregator filtering). Shape: {eod_df_after_fetcher_preprocessing.shape}")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"S3 EOD CUMULATIVE data head (post-fetcher preprocessing):\n{eod_df_after_fetcher_preprocessing.head().to_string()}")

                # Apply product filtering here in the aggregator
                eod_df_filtered = apply_product_filtering(eod_df_after_fetcher_preprocessing, "EOD_CUMULATIVE_AGGREGATOR")
                if eod_df_filtered is not None and not eod_df_filtered.empty:
                    logger.info(f"Successfully filtered EOD CUMULATIVE data in aggregator. Shape: {eod_df_filtered.shape}")
                    all_dataframes.append(eod_df_filtered)
                else:
                    logger.info("EOD CUMULATIVE data was empty after aggregator filtering.")
            else:
                logger.info("EOD CUMULATIVE data was empty after fetcher's preprocessing.")
        else:
            logger.info(f"S3 EOD CUMULATIVE not found or failed for {asset_class} on {target_date.isoformat()}.")

    # 2. Fetch PPD API Slices
    # Only fetch slices for the target_date if EOD data was not successfully processed or was empty.
    # all_dataframes will be empty if EOD failed or resulted in no data after filtering.
    if not all_dataframes:
        logger.info(f"EOD CUMULATIVE data for {target_date.isoformat()} was not found or resulted in no trades. Attempting to fetch PPD API SLICES.")
        logger.info(f"Attempting to fetch PPD API SLICES for {asset_class} on {target_date.isoformat()}")
        current_seq = 1
        # Use config.DTCC_MAX_SEQ_ATTEMPTS_PER_CYCLE or a specific limit for the aggregator
        max_attempts = getattr(config, 'DTCC_MAX_SEQ_ATTEMPTS_PER_CYCLE', 50) 
    
        for attempt in range(max_attempts):
            logger.debug(f"Fetching PPD SLICE: {asset_class}, Date: {target_date.isoformat()}, Seq: {current_seq}")
            try:
                slice_df_raw = dtcc_fetcher.download_and_parse_slice(scraper, asset_class, target_date, current_seq)
                if slice_df_raw is not None and not slice_df_raw.empty:
                    logger.info(f"Successfully fetched PPD SLICE Seq {current_seq}. Raw shape: {slice_df_raw.shape}")
                    slice_df_after_fetcher_preprocessing = dtcc_fetcher.preprocess_dtcc_trades(slice_df_raw.copy(), source_type="SLICE")

                    if slice_df_after_fetcher_preprocessing is not None and not slice_df_after_fetcher_preprocessing.empty:
                        logger.info(f"PPD SLICE Seq {current_seq} after fetcher's preprocessing (before aggregator filtering). Shape: {slice_df_after_fetcher_preprocessing.shape}")
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"PPD SLICE Seq {current_seq} head (post-fetcher preprocessing):\n{slice_df_after_fetcher_preprocessing.head().to_string()}")
                        
                        # Apply product filtering here in the aggregator
                        slice_df_filtered = apply_product_filtering(slice_df_after_fetcher_preprocessing, f"SLICE_AGGREGATOR_SEQ_{current_seq}")
                        if slice_df_filtered is not None and not slice_df_filtered.empty:
                            logger.info(f"Successfully filtered PPD SLICE Seq {current_seq} in aggregator. Shape: {slice_df_filtered.shape}")
                            all_dataframes.append(slice_df_filtered)
                        else:
                            logger.info(f"PPD SLICE Seq {current_seq} was empty after aggregator filtering.")
                    else:
                        logger.info(f"PPD SLICE Seq {current_seq} was empty after fetcher's preprocessing.")
                    current_seq += 1
                else:
                    logger.info(f"PPD SLICE Seq {current_seq} was not found or was empty (raw fetch). Stopping slice fetch for this date/asset.")
                    break 
            except dtcc_fetcher.SliceNotFoundException:
                logger.info(f"No more PPD SLICES for {asset_class} on {target_date.isoformat()} at/after Seq {current_seq}.")
                break
            except Exception as e:
                logger.error(f"Error fetching PPD SLICE {asset_class} {target_date.isoformat()} Seq {current_seq}: {e}", exc_info=True)
                break 
            
            if attempt < max_attempts - 1 and current_seq > 1: # Small delay if continuing to fetch more slices
                 time.sleep(getattr(config, 'DTCC_POLL_INTERVAL_SECONDS_SLICES', 0.5))
    else:
        logger.info(f"Successfully processed EOD CUMULATIVE data for {target_date.isoformat()}. Skipping PPD API Slice fetch for this date.")


    # 3. Aggregate all collected data
    if not all_dataframes:
        logger.warning(f"No data collected for {asset_class} on {target_date.isoformat()}. Returning None.")
        return None

    logger.info(f"Aggregating {len(all_dataframes)} DataFrame(s) for {asset_class} on {target_date.isoformat()}.")
    aggregated_df = pd.concat(all_dataframes, ignore_index=True)
    logger.info(f"Initial aggregated shape: {aggregated_df.shape}")
    
    # Deduplication
    # `preprocess_dtcc_trades` handles amendments within each file.
    # This step handles duplicates that might arise from concatenating EOD and slice data.
    id_cols = ['original_dissemination_id', 'dissemination_id', 'execution_timestamp_utc']
    subset_cols = [col for col in id_cols if col in aggregated_df.columns]
    
    if subset_cols:
        # Sort to ensure consistency if 'keep' matters, though preprocess should give final state.
        # Example: sort by execution_timestamp_utc if available to keep the latest if there were full ID duplicates.
        # However, preprocess_dtcc_trades already aims to get the final state per Original Dissemination ID.
        if 'execution_timestamp_utc' in aggregated_df.columns and 'original_dissemination_id' in aggregated_df.columns:
             aggregated_df.sort_values(by=['original_dissemination_id', 'execution_timestamp_utc'], ascending=[True, True], inplace=True)

        count_before_dedup = len(aggregated_df)
        # Keep 'first' after sorting. If preprocess works perfectly, any duplicate means identical rows.
        aggregated_df.drop_duplicates(subset=subset_cols, keep='first', inplace=True)
        count_after_dedup = len(aggregated_df)
        if count_before_dedup > count_after_dedup:
            logger.info(f"Dropped {count_before_dedup - count_after_dedup} duplicate rows based on {subset_cols}.")
    else:
        # Fallback to all columns if key ID columns are missing (should ideally not happen)
        logger.warning("Key ID columns for deduplication not found. Falling back to all columns for deduplication.")
        count_before_dedup = len(aggregated_df)
        aggregated_df.drop_duplicates(inplace=True)
        count_after_dedup = len(aggregated_df)
        if count_before_dedup > count_after_dedup:
            logger.info(f"Dropped {count_before_dedup - count_after_dedup} duplicate rows based on all columns.")

    logger.info(f"Final aggregated DataFrame shape for {asset_class} on {target_date.isoformat()}: {aggregated_df.shape}")
    if not aggregated_df.empty:
        if logger.isEnabledFor(logging.INFO): # Changed to INFO for head, DEBUG for full string if needed
            logger.info(f"Aggregated Data Head (first 5 rows):\n{aggregated_df.head().to_string()}")
    else:
        logger.info("Aggregated DataFrame is empty.")
        
    return aggregated_df

def apply_product_filtering(df: pd.DataFrame, source_info: str) -> pd.DataFrame | None:
    """
    Applies product-specific filtering (TARGET_UPI, DTCC_PRODUCT_ID_PATTERN) to a DataFrame.
    This function is called within the aggregator after initial preprocessing by the fetcher.
    """
    if df is None or df.empty:
        logger.info(f"Input DataFrame for product filtering is empty ({source_info}). Skipping filtering.")
        return pd.DataFrame()

    logger.info(f"Starting product filtering for {source_info} DataFrame with {df.shape[0]} rows...")

    # Product Filtering Logic (moved from dtcc_fetcher.preprocess_dtcc_trades)
    # Ensure column names are what this filtering logic expects.
    # preprocess_dtcc_trades in fetcher should have already converted them to Title Case.
    # Example: "Unique Product Identifier", "Product Name"

    target_upi = getattr(config, 'TARGET_UPI', None)

    # Check for 'unique_product_identifier' column, which is the snake_case version after fetcher preprocessing
    upi_col_name = "unique_product_identifier" # Renamed by fetcher's preprocess
    product_name_col = "product_name" # Renamed by fetcher's preprocess

    if target_upi:
        if upi_col_name in df.columns:
            logger.info(f"Attempting to filter by TARGET_UPI: {target_upi} on '{upi_col_name}' ({source_info}).")
            original_count = len(df)
            df_filtered_by_upi = df[df[upi_col_name] == target_upi]
            logger.info(f"Found {len(df_filtered_by_upi)} of {original_count} trades after TARGET_UPI filtering ({source_info}).")
            if df_filtered_by_upi.empty:
                logger.info(f"No trades found for TARGET_UPI: {target_upi} ({source_info}). Returning empty DataFrame.")
            # If TARGET_UPI is set and column exists, this is the definitive filter.
            # Return the result of this filtering, whether it's empty or not.
            logger.info(f"Product filtering by TARGET_UPI complete for {source_info}. {len(df_filtered_by_upi)} trades remaining.")
            return df_filtered_by_upi
        else:
            # TARGET_UPI is configured, but the column is missing. Log this and then consider pattern.
            logger.warning(f"TARGET_UPI ('{target_upi}') is configured, but '{upi_col_name}' column not found in {source_info} data. Will attempt pattern filtering if configured.")
            # Fall through to pattern filtering
    else:
        # TARGET_UPI is not configured, so proceed to check for pattern filtering.
        logger.info(f"TARGET_UPI not configured for {source_info}. Will attempt pattern filtering if configured.")

    # Attempt pattern filtering ONLY if TARGET_UPI was not configured,
    # OR if TARGET_UPI was configured but its column was missing.
    # The 'product_filtered_successfully' flag is effectively true if UPI filtering was done and returned.
    # If we reach here, it means UPI filtering was either not applicable or failed due to missing column.
    
    product_id_pattern = getattr(config, 'DTCC_PRODUCT_ID_PATTERN', None)
    if product_id_pattern: # Only attempt if pattern is configured
        if product_name_col in df.columns:
            logger.info(f"Attempting to filter by Product Name pattern: {product_id_pattern} on '{product_name_col}' ({source_info}).")
            original_count = len(df)
            df_filtered_by_pattern = df[df[product_name_col].astype(str).str.contains(product_id_pattern, case=False, na=False, regex=True)]
            logger.info(f"Found {len(df_filtered_by_pattern)} of {original_count} trades after Product Name pattern filtering ({source_info}).")
            if df_filtered_by_pattern.empty:
                logger.info(f"No trades after Product Name pattern filtering ({source_info}): {product_id_pattern}")
            logger.info(f"Product filtering by pattern complete for {source_info}. {len(df_filtered_by_pattern)} trades remaining.")
            return df_filtered_by_pattern
        else:
            logger.warning(f"DTCC_PRODUCT_ID_PATTERN ('{product_id_pattern}') is configured, but '{product_name_col}' column not found for filtering in {source_info}. No product name pattern filtering applied.")
            # If pattern was the only option and its column is missing, return the df as is (no filtering applied)
            logger.info(f"Product filtering complete for {source_info} (no pattern filter applied due to missing column). {len(df)} trades remaining.")
            return df
    else:
        # No TARGET_UPI was successfully applied (either not configured, or column missing) AND no pattern is configured.
        logger.info(f"No TARGET_UPI successfully applied and no DTCC_PRODUCT_ID_PATTERN configured for {source_info}. No product-specific filtering applied by aggregator.")
        logger.info(f"Product filtering complete for {source_info} (no filters applied). {len(df)} trades remaining.")
        return df # Return original df as no filters were applicable or configured
    return df

if __name__ == "__main__":
    logger.info("Starting DTCC Data Aggregator...")

    # Example Usage:
    # Determine target date (e.g., yesterday)
    target_processing_date = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)

    # Use asset classes from config or define specific ones
    asset_classes_to_aggregate = getattr(config, 'DTCC_POLL_ASSET_CLASSES', [])
    if not asset_classes_to_aggregate or not isinstance(asset_classes_to_aggregate, list) or not asset_classes_to_aggregate[0]:
        logger.warning("config.DTCC_POLL_ASSET_CLASSES not configured appropriately. Defaulting to ['CREDITS'] for aggregation.")
        asset_classes_to_aggregate = ["CREDITS"]

    logger.info(f"Configured to aggregate for asset classes: {asset_classes_to_aggregate} on {target_processing_date.isoformat()}")

    all_aggregated_data_map = {} # Store aggregated data per asset class

    for asset_class_item in asset_classes_to_aggregate:
        logger.info(f"--- Processing asset class: {asset_class_item} ---")
        daily_aggregated_df = aggregate_data_for_day(target_processing_date, asset_class_item)
        
        if daily_aggregated_df is not None and not daily_aggregated_df.empty:
            all_aggregated_data_map[asset_class_item] = daily_aggregated_df
            logger.info(f"Successfully aggregated data for {asset_class_item} on {target_processing_date.isoformat()}. Shape: {daily_aggregated_df.shape}")
            
            # Optional: Save the aggregated DataFrame to a file
            # Ensure config.DOWNLOAD_DIR is set in config.py
            # output_base_dir = getattr(config, 'DOWNLOAD_DIR', 'data/dtcc_data')
            # output_dir = pathlib.Path(output_base_dir) / "aggregated_data"
            # output_dir.mkdir(parents=True, exist_ok=True)
            # output_filename = f"aggregated_{asset_class_item.lower()}_{target_processing_date.strftime('%Y%m%d')}.csv"
            # output_filepath = output_dir / output_filename
            # try:
            #     daily_aggregated_df.to_csv(output_filepath, index=False)
            #     logger.info(f"Saved aggregated data to {output_filepath}")
            # except IOError as e:
            #     logger.error(f"Failed to save aggregated data to {output_filepath}: {e}")
        else:
            logger.warning(f"No data aggregated for {asset_class_item} on {target_processing_date.isoformat()}.")

    logger.info("DTCC Data Aggregator finished.")

    # Example: Log summary of all collected data
    if all_aggregated_data_map:
        logger.info("\n--- Aggregation Summary ---")
        for asset, df_summary in all_aggregated_data_map.items():
            logger.info(f"Asset Class '{asset}': {len(df_summary)} records aggregated.")
    else:
        logger.info("No data was aggregated in this run.")
