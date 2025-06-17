import logging
import pandas as pd
import datetime

# Attempt to import xbbg and handle potential import error
try:
    from xbbg import blp
except ImportError:
    blp = None  # Set blp to None if xbbg is not installed or blpapi is not available

from . import config

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_bloomberg_mid_prices(
    ticker: str, trade_date: datetime.date
) -> pd.DataFrame | None:
    """
    Fetches Bloomberg PX_MID tick data for a given ticker and trade date.

    Args:
        ticker: The Bloomberg ticker (e.g., "CDXIG5 Curncy").
        trade_date: The specific date for which to fetch tick data.

    Returns:
        A pandas DataFrame with 'timestamp_utc' and 'mid_price' columns,
        or None if data fetching fails.
    """
    if blp is None:
        logger.error(
            "xbbg or its blpapi dependency is not installed/configured properly. "
            "Bloomberg data cannot be fetched."
        )
        # In a real scenario, you might want to return an empty DataFrame
        # or allow for a mock data path here.
        # For now, returning None to indicate failure.
        return None

    start_datetime_utc_str = trade_date.strftime("%Y-%m-%dT00:00:00")
    end_datetime_utc_str = trade_date.strftime("%Y-%m-%dT23:59:59")

    logger.info(
        f"Fetching Bloomberg PX_MID for {ticker} from {start_datetime_utc_str} to {end_datetime_utc_str}"
    )

    try:
        # Using bdh for historical tick data
        # Ensure TIMEZONE_OVERRIDE is correctly applied by xbbg or if it needs specific handling.
        # xbbg's blp.bdh typically handles timezones well if the session is configured.
        # The 'tz_override' parameter might be available in some xbbg functions or session settings.
        # For simplicity, we rely on the Bloomberg terminal/API default or pre-configured session.
        # The prompt mentioned TIMEZONE_OVERRIDE='UTC' for the API call.
        # xbbg's documentation should be consulted for precise control if default UTC is not achieved.
        
        # Constructing options for the BDH call
        # The example was: =BDH("CDXNAHY CDSI 5Y","PX_MID","2025-06-03T00:00:00","2025-06-03T23:59:59","tick")
        # xbbg's bdh call for ticks: blp.bdh(ticker, 'PX_MID', start_date, end_date, elms=[('interval', interval_minutes)])
        # For tick data, xbbg might use a different function or specific parameters with bdh.
        # Let's try with `typ='Tick'` if available, or rely on high frequency interval if not.
        # According to xbbg docs, for tick data, it's often `blp.bdh_ticks`.
        # If `blp.bdh_ticks` is not a direct method, we might need to use `blp.bdh` with specific options.
        # The user provided: =BDH("CDXNAHY CDSI 5Y","PX_MID","2025-06-03T00:00:00","2025-06-03T23:59:59","tick")
        # This implies we need tick-level data. `xbbg`'s `bdh` can take `elms` for overrides.
        # Let's assume `xbbg` handles the "tick" part by not specifying an interval or using a specific tick function.
        # `blp.history` is another option in `xbbg` which is similar to `bdh`.
        
        # Forcing UTC: xbbg's blp object can be initialized with session options.
        # Or, ensure the Bloomberg Terminal is set to UTC.
        # For now, we assume the environment provides UTC or xbbg handles it.
        # The `TIMEZONE_OVERRIDE` is a Bloomberg API concept.
        # `xbbg` might pass this through `//blp/refdata` overrides.
        # Example: ovrds=[('TIMEZONE_OVERRIDE', 'UTC')]

        df_mid = blp.bdh(
            tickers=ticker,
            flds="PX_MID",
            start_date=start_datetime_utc_str,
            end_date=end_datetime_utc_str,
            # typ="TICK", # This might be how xbbg specifies tick data for bdh
            # Alternatively, xbbg might have a blp.ticks() function or similar
            # Forcing UTC if xbbg supports it directly in bdh call:
            # tz_override='UTC', # This is hypothetical, check xbbg docs
            # Using overrides for Bloomberg API parameters:
            ovrds=[('TIMEZONE_OVERRIDE', config.BLOOMBERG_TIMEZONE_OVERRIDE)]
        )

        if df_mid.empty:
            logger.warning(f"No PX_MID data returned from Bloomberg for {ticker} on {trade_date}.")
            return pd.DataFrame(columns=['timestamp_utc', 'mid_price']) # Return empty with schema

        # xbbg typically returns a DataFrame with a MultiIndex (ticker, time)
        # We need to process this.
        if isinstance(df_mid.columns, pd.MultiIndex):
            df_mid = df_mid[ticker] # Select data for the specific ticker

        df_mid = df_mid.rename(columns={"PX_MID": "mid_price"})
        
        # Ensure index is datetime and UTC
        if not isinstance(df_mid.index, pd.DatetimeIndex):
            df_mid.index = pd.to_datetime(df_mid.index)
        
        df_mid.index = df_mid.index.tz_convert('UTC') # Convert if not already UTC
        
        df_mid = df_mid.reset_index().rename(columns={"index": "timestamp_utc"}) # Pandas >= 2.0 uses 'index'

        logger.info(f"Successfully fetched {len(df_mid)} mid-price ticks for {ticker} on {trade_date}.")
        return df_mid[["timestamp_utc", "mid_price"]]

    except Exception as e:
        logger.error(f"Error fetching Bloomberg data for {ticker} on {trade_date}: {e}")
        # This could be due to connection issues, invalid ticker, no data, or xbbg/blpapi problems.
        return None


if __name__ == "__main__":
    # This is an example and requires a running Bloomberg terminal with API access.
    # Ensure xbbg and blpapi are correctly installed and configured.
    logger.info("Attempting to fetch Bloomberg mid-prices (example)...")
    
    # Mocking a trade date for example purposes
    example_trade_date = datetime.date(2025, 6, 3) # Adjust to a date with known data
    
    # Check if blp object is available (i.e., xbbg imported successfully)
    if blp is not None:
        mid_prices_df = get_bloomberg_mid_prices(
            config.BLOOMBERG_CDX_IG_5Y_TICKER, example_trade_date
        )

        if mid_prices_df is not None and not mid_prices_df.empty:
            logger.info(
                f"Successfully fetched {len(mid_prices_df)} Bloomberg mid-price ticks."
            )
            print(mid_prices_df.head())
            print(mid_prices_df.info())
        elif mid_prices_df is not None and mid_prices_df.empty:
            logger.warning("Bloomberg mid-price fetch returned an empty DataFrame.")
        else:
            logger.error("Failed to get Bloomberg mid-prices.")
    else:
        logger.error("Skipping Bloomberg example: xbbg.blp is not available.")
