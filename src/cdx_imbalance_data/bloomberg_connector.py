import logging
import pandas as pd
import datetime

try:
    from xbbg import blp
except ImportError:
    blp = None

from . import config

logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BloombergConnector:
    def __init__(self):
        if blp is None:
            raise ImportError("xbbg or its blpapi dependency is not installed/configured properly.")
        # The connection is managed by the xbbg library, typically implicitly.
        # If specific session management is needed, it would be handled here.
        logger.info("BloombergConnector initialized.")

    def get_tick_data(self, ticker: str, trade_date: datetime.date) -> pd.DataFrame | None:
        """
        Fetches tick data (trades, bids, and asks) from Bloomberg for a given ticker and date.
        """
        logger.info(f"Fetching Bloomberg ticks for {ticker} on {trade_date.isoformat()}")

        try:
            # Fetch TRADE, BID, and ASK data from Bloomberg
            df_ticks = blp.bdtick(
                ticker=ticker,
                dt=trade_date.strftime('%Y-%m-%d'),
                types=["TRADE", "BID", "ASK"],
                ref='EquityUS'  # Reference exchange, might need adjustment
            )

            if df_ticks.empty:
                logger.warning(f"No tick data returned from Bloomberg for {ticker} on {trade_date}.")
                return pd.DataFrame()

            # Process the MultiIndex columns from bdtick
            security_name = df_ticks.columns.get_level_values(0)[0]
            processed_df = df_ticks.droplevel(0, axis=1).reset_index()
            processed_df.rename(columns={'index': 'timestamp'}, inplace=True)
            processed_df['security'] = security_name
            
            # Make timestamp timezone-aware (Bloomberg ticks are typically in local time of exchange)
            timestamps = pd.to_datetime(processed_df['timestamp'])
            if timestamps.dt.tz is None:
                processed_df['timestamp'] = timestamps.dt.tz_localize('America/New_York')
            else:
                processed_df['timestamp'] = timestamps.dt.tz_convert('America/New_York')

            # Ensure required columns are present
            final_cols = ['timestamp', 'security', 'typ', 'value', 'volume']
            for col in final_cols:
                if col not in processed_df.columns:
                    processed_df[col] = None # Add missing columns with None
            
            final_df = processed_df[final_cols]

            logger.info(f"Successfully fetched {len(final_df)} tick events for {ticker} on {trade_date}.")
            return final_df

        except Exception as e:
            logger.error(f"Error fetching Bloomberg tick data for {ticker} on {trade_date}: {e}", exc_info=True)
            return None

    def stop(self):
        # xbbg manages the session implicitly, but if a session were explicitly started,
        # it would be stopped here.
        logger.info("BloombergConnector stopped.")

if __name__ == "__main__":
    logger.info("Attempting to fetch Bloomberg tick data (example)...")
    
    example_trade_date = datetime.date(2025, 6, 16)
    
    try:
        connector = BloombergConnector()
        ticks_df = connector.get_tick_data(
            config.BLOOMBERG_CDX_IG_5Y_TICKER, example_trade_date
        )

        if ticks_df is not None and not ticks_df.empty:
            logger.info(
                f"Successfully fetched {len(ticks_df)} Bloomberg ticks."
            )
            print(ticks_df.head())
            print(ticks_df.info())
        elif ticks_df is not None and ticks_df.empty:
            logger.warning("Bloomberg tick fetch returned an empty DataFrame.")
        else:
            logger.error("Failed to get Bloomberg ticks.")
        connector.stop()
    except ImportError as e:
        logger.error(f"Skipping Bloomberg example: {e}")
