import datetime as dt
import logging
import time
import zipfile
import io
import pathlib
import argparse
import pandas as pd
import requests
import cloudscraper

from . import config

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SliceNotFoundException(Exception):
    pass

def construct_slice_url(asset_class: str, target_date: dt.date, sequence: int) -> str:
    filename = f"CFTC_SLICE_{asset_class.upper()}_{target_date.strftime('%Y_%m_%d')}_{sequence}.zip"
    return f"{config.DTCC_PPD_API_BASE_URL}{filename}"

def construct_eod_cumulative_url(asset_class: str, target_date: dt.date) -> str:
    filename = f"CFTC_CUMULATIVE_{asset_class.upper()}_{target_date.strftime('%Y_%m_%d')}.zip"
    return f"{config.DTCC_S3_EOD_CUMULATIVE_BASE_URL}{filename}"

def download_and_parse_slice(
    scraper: cloudscraper.CloudScraper,
    asset_class: str,
    target_date: dt.date,
    sequence: int,
) -> pd.DataFrame | None:
    file_url = construct_slice_url(asset_class, target_date, sequence)
    logger.info(f"Attempting to download PPD API SLICE: {file_url} (Seq: {sequence})")
    try:
        response = scraper.get(file_url, timeout=30)
        if response.status_code == 200:
            logger.info(f"Successfully fetched PPD API SLICE {file_url} (Status: 200)")
            zip_content = response.content
            zip_buffer = io.BytesIO(zip_content)
            with zipfile.ZipFile(zip_buffer) as zf:
                if not zf.namelist():
                    logger.error(f"No files found in PPD API SLICE ZIP: {file_url}")
                    return None
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as csv_file:
                    return pd.read_csv(csv_file, low_memory=False, dtype=object)
        elif response.status_code == 404:
            raise SliceNotFoundException(f"PPD API SLICE not found (404): {file_url}")
        else:
            logger.error(f"Failed to download PPD API SLICE {file_url}. Status: {response.status_code}")
            response.raise_for_status()
    except (requests.exceptions.RequestException, cloudscraper.exceptions.CloudflareChallengeError) as e:
        logger.error(f"Request error for PPD API SLICE {file_url}: {e}")
    except zipfile.BadZipFile:
        logger.error(f"Downloaded PPD API SLICE file {file_url} is not a valid zip file.")
    except Exception as e:
        logger.error(f"An unexpected error during PPD API SLICE download/parsing of {file_url}: {e}", exc_info=True)
    return None

def download_and_parse_eod_cumulative_file(
    asset_class: str,
    target_date: dt.date,
) -> pd.DataFrame | None:
    file_url = construct_eod_cumulative_url(asset_class, target_date)
    logger.info(f"Attempting to download S3 EOD CUMULATIVE: {file_url}")
    try:
        response = requests.get(file_url, timeout=60)
        if response.status_code == 200:
            logger.info(f"Successfully fetched S3 EOD CUMULATIVE {file_url} (Status: 200)")
            zip_content = response.content
            zip_buffer = io.BytesIO(zip_content)
            with zipfile.ZipFile(zip_buffer) as zf:
                if not zf.namelist():
                    logger.error(f"No files found in S3 EOD CUMULATIVE ZIP: {file_url}")
                    return None
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as csv_file:
                    return pd.read_csv(csv_file, low_memory=False, dtype=object)
        elif response.status_code != 404:
            logger.error(f"Failed to download S3 EOD CUMULATIVE {file_url}. Status: {response.status_code}")
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for S3 EOD CUMULATIVE {file_url}: {e}")
    except zipfile.BadZipFile:
        logger.error(f"Downloaded S3 EOD CUMULATIVE file {file_url} is not a valid zip file.")
    except Exception as e:
        logger.error(f"An unexpected error during S3 EOD CUMULATIVE download/parsing of {file_url}: {e}", exc_info=True)
    return None

def preprocess_dtcc_trades(df: pd.DataFrame | None, source_type: str = "SLICE") -> pd.DataFrame | None:
    if df is None or df.empty:
        logger.warning(f"Input DataFrame is empty for preprocessing ({source_type}).")
        return pd.DataFrame()

    logger.info(f"Starting preprocessing for {source_type} DataFrame with {df.shape[0]} rows...")
    
    rename_map = {col: col.replace(" ", "_").upper() for col in df.columns} 
    df.rename(columns=rename_map, inplace=True)
    
    title_case_rename_map = {col: " ".join(word.capitalize() for word in col.split("_")) for col in df.columns}
    df.rename(columns=title_case_rename_map, inplace=True)
    
    required_cols = [
        "Execution Timestamp", "Dissemination Identifier", "Original Dissemination Identifier",
        "Product Name", "Price", "Price Notation", "Spread-leg 1", "Spread Notation-leg 1",
        "Notional Amount-leg 1", "Notional Amount Currency-leg 1", "Action Type", "Event Type"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"Potentially missing columns for preprocessing ({source_type}). Required but not found: {missing_cols}.")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Available columns in DataFrame for {source_type} at this stage ({len(df.columns)} total): {list(df.columns)}")

    if "Execution Timestamp" in df.columns:
        df["Execution Timestamp"] = pd.to_datetime(df["Execution Timestamp"], errors='coerce', utc=True)
        df.dropna(subset=["Execution Timestamp"], inplace=True)
    else:
        logger.warning(f"Column 'Execution Timestamp' not found for conversion in preprocessing ({source_type}).")

    if df.empty :
        logger.info(f"DataFrame is empty before amendment handling. ({source_type})")

    action_column_name = "Action Type" 
    if "Original Dissemination ID" in df.columns and "Execution Timestamp" in df.columns and action_column_name in df.columns:
        df.sort_values(by=["Original Dissemination ID", "Execution Timestamp"], ascending=[True, True], inplace=True)
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

    column_map_to_final = {
        "Execution Timestamp": "execution_timestamp_utc", "Dissemination Identifier": "dissemination_id",
        "Original Dissemination Identifier": "original_dissemination_id",
        "Product Name": "product_name",
        "Price": "price_value", "Price Notation": "price_notation", 
        "Spread-leg 1": "spread_value",
        "Notional Amount-leg 1": "notional_amount",
        "Notional Amount Currency-leg 1": "notional_currency",
        "Action Type": "action_type", "Event Type": "event_type",
        "Unique Product Identifier": "unique_product_identifier"
    }
    
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

    logger.info(f"Preprocessing complete for {source_type}. {len(df_renamed)} trades ready.")
    return df_renamed

def backfill_data(start_date: dt.date, end_date: dt.date):
    logger.info(f"Starting backfill from {start_date.isoformat()} to {end_date.isoformat()}")
    scraper = cloudscraper.create_scraper()
    all_backfill_data = []

    for day_offset in range((end_date - start_date).days + 1):
        current_date = start_date + dt.timedelta(days=day_offset)
        for asset_class in config.DTCC_POLL_ASSET_CLASSES:
            logger.info(f"Processing backfill for {asset_class} on {current_date.isoformat()}")
            eod_df = download_and_parse_eod_cumulative_file(asset_class, current_date)
            if eod_df is not None and not eod_df.empty:
                logger.info(f"Successfully processed S3 EOD CUMULATIVE for {asset_class} {current_date.isoformat()}")
                all_backfill_data.append(eod_df)
                continue

            logger.info(f"EOD not found for {current_date.isoformat()}, fetching slices.")
            current_seq = 1
            while True:
                try:
                    slice_df = download_and_parse_slice(scraper, asset_class, current_date, current_seq)
                    if slice_df is not None and not slice_df.empty:
                        all_backfill_data.append(slice_df)
                        current_seq += 1
                    else:
                        break
                except SliceNotFoundException:
                    logger.info(f"No more slices for {asset_class} on {current_date.isoformat()} at/after Seq {current_seq}.")
                    break
                except Exception as e:
                    logger.error(f"Error fetching slice {current_seq} for {current_date.isoformat()}: {e}", exc_info=True)
                    break
                time.sleep(0.5)

    if all_backfill_data:
        final_df = pd.concat(all_backfill_data, ignore_index=True)
        output_filename = f"backfill_{start_date.isoformat()}_to_{end_date.isoformat()}.parquet"
        output_path = pathlib.Path(config.DATA_DIR) / output_filename
        final_df.to_parquet(output_path)
        logger.info(f"Backfill complete. Saved {len(final_df)} records to {output_path}")
    else:
        logger.info("Backfill complete. No data found for the specified date range.")

def poll_for_new_slices():
    logger.info("Starting continuous polling for new slices...")
    scraper = cloudscraper.create_scraper()
    all_polled_data = []
    last_processed_sequences = {}

    try:
        while True:
            current_utc_date = dt.datetime.now(dt.timezone.utc).date()
            new_data_found_in_cycle = False
            for asset_class in config.DTCC_POLL_ASSET_CLASSES:
                seq_key = f"{current_utc_date.strftime('%Y%m%d')}_{asset_class.upper()}"
                current_seq = last_processed_sequences.get(seq_key, 0) + 1
                
                try:
                    df = download_and_parse_slice(scraper, asset_class, current_utc_date, current_seq)
                    if df is not None and not df.empty:
                        all_polled_data.append(df)
                        last_processed_sequences[seq_key] = current_seq
                        new_data_found_in_cycle = True
                except SliceNotFoundException:
                    pass
                except Exception as e:
                    logger.error(f"Polling Error for {asset_class} Seq {current_seq}: {e}", exc_info=True)
            
            if not new_data_found_in_cycle:
                logger.debug("No new slices found in this polling cycle.")
            
            time.sleep(config.DTCC_POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Polling stopped by user.")
    finally:
        if all_polled_data:
            final_df = pd.concat(all_polled_data, ignore_index=True)
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"polling_session_{timestamp}.parquet"
            output_path = pathlib.Path(config.DATA_DIR) / output_filename
            final_df.to_parquet(output_path)
            logger.info(f"Polling session ended. Saved {len(final_df)} records to {output_path}")
        else:
            logger.info("Polling session ended. No new data was collected.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DTCC Data Fetcher")
    subparsers = parser.add_subparsers(dest="mode", required=True, help="The mode to run the fetcher in.")

    backfill_parser = subparsers.add_parser("backfill", help="Fetch historical data for a date range.")
    backfill_parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format.")
    backfill_parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format.")

    poll_parser = subparsers.add_parser("poll", help="Poll for new intraday data continuously.")

    args = parser.parse_args()

    if args.mode == "backfill":
        start = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        backfill_data(start, end)
    elif args.mode == "poll":
        poll_for_new_slices()
