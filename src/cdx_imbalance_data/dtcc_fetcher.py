import datetime as dt
import logging
import time
import zipfile
import io # For BytesIO
import pathlib # For path manipulation

import pandas as pd
import requests # For S3 EOD downloads and requests.exceptions
import cloudscraper # Import the cloudscraper fork

from . import config

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Custom Exception for File Not Found (404) to differentiate from other errors
class SliceNotFoundException(Exception):
    pass

def construct_slice_url(asset_class: str, target_date: dt.date, sequence: int) -> str:
    """
    Constructs the URL for a DTCC PPD API slice file.
    """
    filename = f"CFTC_SLICE_{asset_class.upper()}_{target_date.strftime('%Y_%m_%d')}_{sequence}.zip"
    return f"{config.DTCC_PPD_API_BASE_URL}{filename}"

def construct_eod_cumulative_url(asset_class: str, target_date: dt.date) -> str:
    """
    Constructs the URL for an S3 EOD cumulative file.
    Example: https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/CFTC_CUMULATIVE_CREDITS_2024_01_01.zip
    """
    filename = f"CFTC_CUMULATIVE_{asset_class.upper()}_{target_date.strftime('%Y_%m_%d')}.zip"
    return f"{config.DTCC_S3_EOD_CUMULATIVE_BASE_URL}{filename}"

def download_and_parse_slice(
    scraper: cloudscraper.CloudScraper,
    asset_class: str,
    target_date: dt.date,
    sequence: int,
) -> pd.DataFrame | None:
    """
    Downloads a specific PPD API slice ZIP, extracts CSV, and parses it.
    """
    file_url = construct_slice_url(asset_class, target_date, sequence)
    logger.info(f"Attempting to download PPD API SLICE: {file_url} (Seq: {sequence})")
    zip_filepath = None # Initialize zip_filepath

    try:
        response = scraper.get(file_url, timeout=30)
        if response.status_code == 200:
            logger.info(f"Successfully fetched PPD API SLICE {file_url} (Status: 200)")
            zip_content = response.content
            
            if config.SAVE_DOWNLOADED_FILES:
                download_path = pathlib.Path(config.DOWNLOAD_DIR) / config.SLICES_DOWNLOAD_SUBDIR / asset_class / target_date.strftime("%Y-%m-%d")
                download_path.mkdir(parents=True, exist_ok=True)
                zip_filename = file_url.split("/")[-1]
                zip_filepath = download_path / zip_filename
                try:
                    zip_filepath.write_bytes(zip_content)
                    logger.info(f"Saved PPD API SLICE ZIP to: {zip_filepath} ({len(zip_content):,} bytes)")
                except IOError as e:
                    logger.error(f"Failed to save PPD API SLICE ZIP file {zip_filepath}: {e}")

            zip_buffer = io.BytesIO(zip_content)
            with zipfile.ZipFile(zip_buffer) as zf:
                if not zf.namelist():
                    logger.error(f"No files found in PPD API SLICE ZIP: {file_url}")
                    return None
                csv_name = zf.namelist()[0]
                logger.info(f"Extracting CSV: {csv_name} from PPD API SLICE {file_url.split('/')[-1]}")
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file, low_memory=False, dtype=object)
                    logger.info(f"Loaded {len(df)} records from PPD API SLICE CSV {csv_name}")
                    if zip_filepath: # Check if zip_filepath was set (i.e., config.SAVE_DOWNLOADED_FILES was true and file saved)
                        csv_filepath = zip_filepath.with_suffix(".csv")
                        try:
                            df.to_csv(csv_filepath, index=False)
                            logger.info(f"Saved extracted PPD API SLICE CSV to: {csv_filepath}")
                        except IOError as e:
                            logger.error(f"Failed to save PPD API SLICE CSV file {csv_filepath}: {e}")
                    return df
        elif response.status_code == 404:
            logger.info(f"PPD API SLICE not found (404): {file_url}")
            raise SliceNotFoundException(f"PPD API SLICE not found (404): {file_url}")
        elif response.status_code == 403:
            logger.warning(f"Cloudflare challenge likely failed (403) for PPD API SLICE {file_url}. Content: {response.text[:200]}")
            return None
        else:
            logger.error(f"Failed to download PPD API SLICE {file_url}. Status: {response.status_code}, Content: {response.text[:200]}")
            response.raise_for_status()
            return None
    except cloudscraper.exceptions.CloudflareChallengeError as e:
        logger.error(f"Cloudflare challenge error for PPD API SLICE {file_url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for PPD API SLICE {file_url}: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Error: Downloaded PPD API SLICE file {file_url} is not a valid zip file or is corrupted.")
        return None
    except SliceNotFoundException:
        raise
    except Exception as e:
        logger.error(f"An unexpected error during PPD API SLICE download/parsing of {file_url}: {e}", exc_info=True)
        return None

def download_and_parse_eod_cumulative_file(
    asset_class: str,
    target_date: dt.date,
) -> pd.DataFrame | None:
    """
    Downloads a specific S3 EOD cumulative ZIP, extracts CSV, and parses it.
    """
    file_url = construct_eod_cumulative_url(asset_class, target_date)
    logger.info(f"Attempting to download S3 EOD CUMULATIVE: {file_url}")
    zip_filepath = None # Initialize zip_filepath

    try:
        response = requests.get(file_url, timeout=60) # Standard requests for S3
        if response.status_code == 200:
            logger.info(f"Successfully fetched S3 EOD CUMULATIVE {file_url} (Status: 200)")
            zip_content = response.content

            if config.SAVE_DOWNLOADED_FILES:
                download_path = pathlib.Path(config.DOWNLOAD_DIR) / config.EOD_DOWNLOAD_SUBDIR / asset_class / target_date.strftime("%Y-%m-%d")
                download_path.mkdir(parents=True, exist_ok=True)
                zip_filename = file_url.split("/")[-1]
                zip_filepath = download_path / zip_filename
                try:
                    zip_filepath.write_bytes(zip_content)
                    logger.info(f"Saved S3 EOD CUMULATIVE ZIP to: {zip_filepath} ({len(zip_content):,} bytes)")
                except IOError as e:
                    logger.error(f"Failed to save S3 EOD CUMULATIVE ZIP file {zip_filepath}: {e}")
            
            zip_buffer = io.BytesIO(zip_content)
            with zipfile.ZipFile(zip_buffer) as zf:
                if not zf.namelist():
                    logger.error(f"No files found in S3 EOD CUMULATIVE ZIP: {file_url}")
                    return None
                # Assuming one primary CSV, or you might need more logic to find the correct one
                csv_name = zf.namelist()[0] 
                logger.info(f"Extracting CSV: {csv_name} from S3 EOD CUMULATIVE {file_url.split('/')[-1]}")
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file, low_memory=False, dtype=object)
                    logger.info(f"Loaded {len(df)} records from S3 EOD CUMULATIVE CSV {csv_name}")
                    if zip_filepath: # Check if zip_filepath was set (i.e., config.SAVE_DOWNLOADED_FILES was true and file saved)
                        csv_filepath = zip_filepath.with_suffix(".csv")
                        try:
                            df.to_csv(csv_filepath, index=False)
                            logger.info(f"Saved extracted S3 EOD CUMULATIVE CSV to: {csv_filepath}")
                        except IOError as e:
                            logger.error(f"Failed to save S3 EOD CUMULATIVE CSV file {csv_filepath}: {e}")
                    return df
        elif response.status_code == 404:
            logger.info(f"S3 EOD CUMULATIVE file not found (404): {file_url}")
            return None # No special exception, just return None for EOD
        else:
            logger.error(f"Failed to download S3 EOD CUMULATIVE {file_url}. Status: {response.status_code}")
            response.raise_for_status()
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for S3 EOD CUMULATIVE {file_url}: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Error: Downloaded S3 EOD CUMULATIVE file {file_url} is not a valid zip file or is corrupted.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error during S3 EOD CUMULATIVE download/parsing of {file_url}: {e}", exc_info=True)
        return None

def preprocess_dtcc_trades(df: pd.DataFrame | None, source_type: str = "SLICE") -> pd.DataFrame | None:
    """
    Preprocesses the raw DTCC DataFrame. Source_type can be "SLICE" or "EOD_CUMULATIVE".
    """
    if df is None or df.empty:
        logger.warning(f"Input DataFrame is empty for preprocessing ({source_type}).")
        return pd.DataFrame()

    logger.info(f"Starting preprocessing for {source_type} DataFrame with {df.shape[0]} rows...")
    
    # Standardize column names (example: PPD API often uses all caps)
    # This needs to be verified with actual slice data from PPD API / S3 EOD
    rename_map = {col: col.replace(" ", "_").upper() for col in df.columns} 
    df.rename(columns=rename_map, inplace=True)
    
    # Column name mapping might differ between slices and EOD files.
    # This section needs careful verification against actual data from both sources.
    # For now, using a generic approach.
    # Example: if EOD uses "Execution_Timestamp" and slices use "EXECUTION_TIMESTAMP"
    # The rename_map above standardizes to "EXECUTION_TIMESTAMP".
    # The preprocess_dtcc_trades function then expects title case like "Execution Timestamp".
    # We need to ensure the df passed to the core logic of preprocess_dtcc_trades has consistent names.

    # Convert standardized (UPPER_SNAKE_CASE) to Title Case for consistency with original preprocess logic
    title_case_rename_map = {col: " ".join(word.capitalize() for word in col.split("_")) for col in df.columns}
    df.rename(columns=title_case_rename_map, inplace=True)
    
    # Original required_cols and column_map used Title Case.
    # Adjusted based on user-provided column list and observed title-casing behavior.
    required_cols = [
        "Execution Timestamp", "Dissemination Identifier", "Original Dissemination Identifier", # Changed ID to Identifier
        "Product Name", "Price", "Price Notation", "Spread-leg 1", "Spread Notation-leg 1", # Changed Leg to leg
        "Notional Amount-leg 1", "Notional Amount Currency-leg 1", "Action Type", "Event Type" # Changed Leg to leg
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Potentially missing columns for preprocessing ({source_type}). Required but not found: {missing_cols}.")
        if logger.isEnabledFor(logging.DEBUG): # Log available columns only if DEBUG is enabled
            logger.debug(f"Available columns in DataFrame for {source_type} at this stage ({len(df.columns)} total): {list(df.columns)}")

    if "Execution Timestamp" in df.columns:
        df["Execution Timestamp"] = pd.to_datetime(df["Execution Timestamp"], errors='coerce', utc=True)
        df.dropna(subset=["Execution Timestamp"], inplace=True)
    else:
        logger.warning(f"Column 'Execution Timestamp' not found for conversion in preprocessing ({source_type}).")

    # Product filtering has been moved to the aggregator.
    # This function now focuses on generic preprocessing like column name standardization,
    # date conversion, and amendment handling.

    if df.empty :
        logger.info(f"DataFrame is empty before amendment handling. ({source_type})")
        # Amendment handling can proceed with an empty df and will produce an empty DF.

    # Use "Action Type" based on user feedback and expected title-casing
    action_column_name = "Action Type" 
    if "Original Dissemination ID" in df.columns and "Execution Timestamp" in df.columns and action_column_name in df.columns:
        df.sort_values(by=["Original Dissemination ID", "Execution Timestamp"], ascending=[True, True], inplace=True)
        # Ensure 'Action Type' column exists before trying to access it in the groupby operation
        # and that the group is not empty.
        final_trades = []
        for _, group in df.groupby("Original Dissemination ID"):
            if not group.empty and action_column_name in group.columns and group.iloc[-1][action_column_name] not in ["CANCEL", "ERROR"]:
                final_trades.append(group.iloc[-1])
        
        if not final_trades:
            logger.info(f"No valid trades after amendment handling ({source_type}).")
            return pd.DataFrame()
        df = pd.DataFrame(final_trades)
        logger.info(f"{len(df)} trades after amendment handling ({source_type}).")
    else:
        logger.warning(f"Missing columns for amendment handling in preprocessing ({source_type}). Skipping.")

    # Adjusted keys "Action Type" and "Event Type" based on user feedback and expected title-casing
    # Added "Unique Product Identifier" to ensure it's preserved for filtering in the aggregator.
    # Adjusted keys to match the output of title_case_rename_map logic.
    column_map_to_final = {
        "Execution Timestamp": "execution_timestamp_utc", "Dissemination Identifier": "dissemination_id",
        "Original Dissemination Identifier": "original_dissemination_id", # Changed ID to Identifier
        "Product Name": "product_name",
        "Price": "price_value", "Price Notation": "price_notation", 
        "Spread-leg 1": "spread_value", # Changed Leg to leg
        "Spread Notation-leg 1": "spread_notation", # Changed Leg to leg
        "Notional Amount-leg 1": "notional_amount", # Changed Leg to leg
        "Notional Amount Currency-leg 1": "notional_currency", # Changed Leg to leg
        "Action Type": "action_type", "Event Type": "event_type",
        "Unique Product Identifier": "unique_product_identifier"
    }
    
    # Ensure all columns needed for downstream processing are selected, even if not explicitly in column_map_to_final's values
    # For now, the explicit mapping in column_map_to_final and then selecting its keys is the primary mechanism.
    # If other columns are needed raw, this selection logic might need adjustment.
    # However, the current issue is that "Unique Product Identifier" was not a key, so it was dropped.
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Columns in df before final selection for {source_type} ({len(df.columns)} total): {list(df.columns)}")

    cols_to_select = [col for col in column_map_to_final.keys() if col in df.columns]
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Columns selected by column_map_to_final for {source_type}: {cols_to_select}")
        missing_from_map_but_in_df = [col for col in df.columns if col not in column_map_to_final.keys()]
        if missing_from_map_but_in_df:
            logger.debug(f"Columns in df but NOT in column_map_to_final.keys() (will be dropped): {missing_from_map_but_in_df}")

    df_renamed = df[cols_to_select].rename(columns=column_map_to_final)

    if 'notional_amount' in df_renamed.columns:
        df_renamed['notional_amount'] = pd.to_numeric(df_renamed['notional_amount'], errors='coerce')

    if 'spread_value' in df_renamed.columns:
        df_renamed['spread_value'] = pd.to_numeric(df_renamed['spread_value'], errors='coerce')
        df_renamed['spread_value'] = df_renamed['spread_value'] * 10000
        logger.info("Converted 'spread_value' to basis points (multiplied by 10000).")

    logger.info(f"Preprocessing complete for {source_type}. {len(df_renamed)} trades ready.")
    return df_renamed

def poll_for_new_slices() -> None:
    logger.info("Initializing DTCC Data Fetcher (PPD API Slices and S3 EOD Cumulative)...")
    
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True, "version": config.CS_BROWSER_VERSION}
    )
    scraper.headers.update({"Referer": config.CS_REFERER_URL})
    logger.info(f"Cloudscraper initialized for PPD API Slices with browser version: {config.CS_BROWSER_VERSION}")

    if config.SAVE_DOWNLOADED_FILES:
        pathlib.Path(config.DOWNLOAD_DIR, config.SLICES_DOWNLOAD_SUBDIR).mkdir(parents=True, exist_ok=True)
        pathlib.Path(config.DOWNLOAD_DIR, config.EOD_DOWNLOAD_SUBDIR).mkdir(parents=True, exist_ok=True)
        logger.info(f"Will save downloaded files to subdirectories within: {pathlib.Path(config.DOWNLOAD_DIR).resolve()}")

    last_processed_sequences: dict[str, int] = {} # For PPD API Slices: { "YYYYMMDD_ASSETCLASS": last_successful_sequence }

    # Initial scan for lookback days
    if config.DTCC_INITIAL_LOOKBACK_DAYS >= 0:
        for day_offset in range(config.DTCC_INITIAL_LOOKBACK_DAYS, -1, -1): # From oldest to newest
            lookback_date = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=day_offset)
            
            for asset_class in config.DTCC_POLL_ASSET_CLASSES:
                logger.info(f"Processing lookback for {asset_class} on {lookback_date.isoformat()}")

                # For true past days (day_offset > 0), try fetching EOD cumulative first if enabled
                if day_offset > 0 and config.FETCH_EOD_CUMULATIVE_FOR_LOOKBACK:
                    logger.info(f"Attempting to fetch S3 EOD CUMULATIVE for {asset_class} on {lookback_date.isoformat()}")
                    eod_df = download_and_parse_eod_cumulative_file(asset_class, lookback_date)
                    if eod_df is not None and not eod_df.empty:
                        logger.info(f"Successfully processed S3 EOD CUMULATIVE for {asset_class} {lookback_date.isoformat()}. Shape: {eod_df.shape}")
                        print(f"\n--- S3 EOD CUMULATIVE Data: {asset_class} {lookback_date.isoformat()} ---")
                        print(eod_df.head())
                        # processed_eod_df = preprocess_dtcc_trades(eod_df.copy(), source_type="EOD_CUMULATIVE")
                        # if processed_eod_df is not None and not processed_eod_df.empty:
                        # print(f"--- Processed S3 EOD: {processed_eod_df.head()}")
                        continue # Move to next asset_class/day if EOD was successful for this past day
                    else:
                        logger.info(f"S3 EOD CUMULATIVE not found or failed for {asset_class} on {lookback_date.isoformat()}. Will try PPD API slices if applicable.")
                
                # For "today" (day_offset == 0) or if EOD failed/disabled for past days, try PPD API slices
                logger.info(f"Scanning PPD API SLICES for {asset_class} on {lookback_date.isoformat()} (part of lookback/initialization)")
                seq_key = f"{lookback_date.strftime('%Y%m%d')}_{asset_class.upper()}"
                current_seq = 1 # Start from sequence 1
                
                for _ in range(config.DTCC_MAX_SEQ_ATTEMPTS_PER_CYCLE):
                    logger.debug(f"Lookback/Initial PPD SLICE: Trying {asset_class} for {lookback_date.isoformat()}, Seq: {current_seq}")
                    try:
                        df = download_and_parse_slice(scraper, asset_class, lookback_date, current_seq)
                        if df is not None and not df.empty:
                            logger.info(f"Lookback/Initial PPD SLICE: Successfully processed {asset_class} {lookback_date.isoformat()} Seq {current_seq}. Shape: {df.shape}")
                            print(f"\n--- Lookback/Initial PPD SLICE Data: {asset_class} {lookback_date.isoformat()} Seq {current_seq} ---")
                            print(df.head())
                            # processed_df = preprocess_dtcc_trades(df.copy(), source_type="SLICE")
                            last_processed_sequences[seq_key] = current_seq
                            current_seq += 1
                        else:
                            break 
                    except SliceNotFoundException:
                        logger.info(f"Lookback/Initial PPD SLICE: No more slices for {asset_class} on {lookback_date.isoformat()} at/after Seq {current_seq}.")
                        break 
                    except Exception as e:
                        logger.error(f"Lookback/Initial PPD SLICE: Error for {asset_class} {lookback_date.isoformat()} Seq {current_seq}: {e}", exc_info=True)
                        break 
                    time.sleep(0.5) 
                time.sleep(1) 
    
    logger.info("Initial scan complete. Starting continuous PPD API SLICE polling for current day.")
    try:
        while True:
            current_utc_date = dt.datetime.now(dt.timezone.utc).date()
            new_data_found_in_cycle = False

            for asset_class in config.DTCC_POLL_ASSET_CLASSES:
                seq_key = f"{current_utc_date.strftime('%Y%m%d')}_{asset_class.upper()}"
                current_seq = last_processed_sequences.get(seq_key, 0) + 1 
                
                logger.debug(f"Polling PPD SLICE: Checking {asset_class} for {current_utc_date.isoformat()}, starting from Seq: {current_seq}")

                for _ in range(config.DTCC_MAX_SEQ_ATTEMPTS_PER_CYCLE):
                    logger.debug(f"Polling PPD SLICE: Trying {asset_class} for {current_utc_date.isoformat()}, Seq: {current_seq}")
                    try:
                        df = download_and_parse_slice(scraper, asset_class, current_utc_date, current_seq)
                        if df is not None and not df.empty:
                            logger.info(f"Polling PPD SLICE: Successfully processed {asset_class} {current_utc_date.isoformat()} Seq {current_seq}. Shape: {df.shape}")
                            print(f"\n--- Polled PPD SLICE Data: {asset_class} {current_utc_date.isoformat()} Seq {current_seq} ---")
                            print(df.head())
                            # processed_df = preprocess_dtcc_trades(df.copy(), source_type="SLICE")
                            last_processed_sequences[seq_key] = current_seq
                            new_data_found_in_cycle = True
                            current_seq += 1 
                        else:
                            break 
                    except SliceNotFoundException:
                        logger.debug(f"Polling PPD SLICE: No slice for {asset_class} {current_utc_date.isoformat()} Seq {current_seq} (404).")
                        break 
                    except Exception as e:
                        logger.error(f"Polling PPD SLICE: Error for {asset_class} {current_utc_date.isoformat()} Seq {current_seq}: {e}", exc_info=True)
                        break 
                    if _ > 0 : time.sleep(0.2) # Small delay only if trying multiple sequences in a row for current day
            
            if not new_data_found_in_cycle:
                logger.debug(f"No new PPD API SLICES found in this polling cycle ending {dt.datetime.now(dt.timezone.utc)}.")
            
            time.sleep(config.DTCC_POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Polling stopped by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the polling loop: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("Starting DTCC Data Fetcher (PPD API Slices and S3 EOD Cumulative)...")
    if not config.DTCC_POLL_ASSET_CLASSES or not config.DTCC_POLL_ASSET_CLASSES[0]:
        logger.warning("DTCC_POLL_ASSET_CLASSES not configured appropriately, defaulting to ['CREDITS'].")
    logger.info(f"Configured to poll for asset classes: {config.DTCC_POLL_ASSET_CLASSES}")
    try:
        poll_for_new_slices()
    except Exception as e:
        logger.critical(f"Failed to start poller: {e}", exc_info=True)
