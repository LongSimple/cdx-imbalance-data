import logging
import pandas as pd
import numpy as np

from . import config

# Configure logging
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def convert_spread_to_price(
    spread_bps: float,
    years_to_maturity: float = 5.0, # Assuming 5Y for CDX IG
    coupon_bps: float = config.STANDARD_CDX_IG_COUPON * 10000, # e.g., 100 bps for 1%
    recovery_rate: float = config.STANDARD_RECOVERY_RATE # e.g., 0.4 for 40%
) -> float | None:
    """
    Converts a CDS spread (in basis points) to a price (points upfront).
    This is a placeholder and requires a proper financial model (e.g., ISDA model).
    A simplified model might look like: Price = (Coupon - Spread) * DV01_approx + Par
    Or more commonly for upfront: (Coupon_bps - Spread_bps) * Annuity_Factor / 10000 * Par + Par
    where Par is 100. The actual calculation is more complex.

    Args:
        spread_bps: The CDS spread in basis points.
        years_to_maturity: Time to maturity in years.
        coupon_bps: The standard coupon of the CDS in basis points.
        recovery_rate: The assumed recovery rate (e.g., 0.4 for 40%).

    Returns:
        The price in points upfront, or None if conversion fails.
    """
    logger.warning(
        "Placeholder function `convert_spread_to_price` called. "
        "Implement actual financial logic for accurate conversion."
    )
    # This is a highly simplified and likely incorrect placeholder.
    # For example, a rough approximation for upfront points:
    # upfront_points = (coupon_bps - spread_bps) * years_to_maturity * 0.01 # Very rough
    # price = 100 + upfront_points
    # DO NOT USE THIS IN PRODUCTION.
    # A proper model would involve calculating risky PV of fixed leg and floating leg.
    if spread_bps is None:
        return None
    # Returning NaN to indicate that a proper conversion is needed.
    return np.nan # Placeholder: requires real financial model


def convert_price_to_spread(
    price: float,
    years_to_maturity: float = 5.0,
    coupon_bps: float = config.STANDARD_CDX_IG_COUPON * 10000,
    recovery_rate: float = config.STANDARD_RECOVERY_RATE
) -> float | None:
    """
    Converts a CDS price (points upfront) to a spread (in basis points).
    This is a placeholder and requires a proper financial model (e.g., ISDA model via root finding).

    Args:
        price: The CDS price in points upfront (e.g., 101.5 for 1.5 points upfront).
        years_to_maturity: Time to maturity in years.
        coupon_bps: The standard coupon of the CDS in basis points.
        recovery_rate: The assumed recovery rate.

    Returns:
        The spread in basis points, or None if conversion fails.
    """
    logger.warning(
        "Placeholder function `convert_price_to_spread` called. "
        "Implement actual financial logic for accurate conversion."
    )
    # This is a highly simplified and likely incorrect placeholder.
    # upfront_points = price - 100
    # spread_bps_approx = coupon_bps - (upfront_points / (years_to_maturity * 0.01)) # Very rough
    # DO NOT USE THIS IN PRODUCTION.
    if price is None:
        return None
    # Returning NaN to indicate that a proper conversion is needed.
    return np.nan # Placeholder: requires real financial model


def normalize_dtcc_quote(row: pd.Series) -> tuple[str | None, float | None, str | None]:
    """
    Determines the quote basis (price or spread) and its value from a DTCC trade row.
    Also handles notation (e.g., BPS, DECIMAL_PERCENTAGE).

    Args:
        row: A pandas Series representing a single DTCC trade.

    Returns:
        A tuple: (quote_basis: str, quote_value: float, original_notation: str).
        quote_basis can be 'price', 'spread', or None.
        quote_value is the numeric value, adjusted for notation if possible.
        original_notation is the notation string from the data.
    """
    price_val_str = str(row.get("price_value", "")).strip()
    price_notation = str(row.get("price_notation", "")).strip().upper()
    spread_val_str = str(row.get("spread_value", "")).strip()
    spread_notation = str(row.get("spread_notation", "")).strip().upper()

    quote_basis = None
    quote_value = np.nan
    original_notation = None

    # Prefer spread if both are somehow present, though typically one is NaN/empty
    if pd.notna(spread_val_str) and spread_val_str != "":
        try:
            val = float(spread_val_str)
            original_notation = spread_notation
            quote_value = val
            quote_basis = "spread"
        except ValueError:
            logger.warning(f"Could not convert spread_value '{spread_val_str}' to float.")
            pass # Fall through to check price

    if quote_basis is None and pd.notna(price_val_str) and price_val_str != "":
        try:
            val = float(price_val_str)
            original_notation = price_notation
            # Price is usually in points (e.g., 101.5 means 101.5% of notional)
            # Or points upfront (e.g. 1.5 for 1.5 points upfront on par of 100)
            # PPD guide: "Price Notation" (page 13) - e.g. "DECIMAL", "PERCENTAGE_OF_NOMINAL"
            # Assuming "Price" field is directly comparable or is points upfront.
            # If it's full price (e.g. 98.5), it might need to be converted to points upfront from par (100).
            # For now, assume the value is directly usable as "points".
            quote_value = val
            quote_basis = "price"
        except ValueError:
            logger.warning(f"Could not convert price_value '{price_val_str}' to float.")
            pass

    if quote_basis is None:
        logger.debug(f"Could not determine quote basis for trade: {row.get('dissemination_id')}")

    return quote_basis, quote_value, original_notation

if __name__ == "__main__":
    logger.info("Economic Conversions Module - Example Usage")

    # Example: Spread to Price (Placeholder)
    spread1 = 70.0  # bps
    price1 = convert_spread_to_price(spread1)
    logger.info(f"Spread {spread1} bps -> Price (placeholder): {price1}")

    # Example: Price to Spread (Placeholder)
    price2 = 1.5  # Points upfront (e.g. 101.5 on par 100, or just 1.5)
    spread2 = convert_price_to_spread(price2)
    logger.info(f"Price {price2} pts -> Spread (placeholder): {spread2} bps")

    # Example: Normalize DTCC Quote
    example_trade_spread = pd.Series({
        "dissemination_id": "EX001",
        "price_value": None, "price_notation": None,
        "spread_value": "75.5", "spread_notation": "BPS"
    })
    basis, val, notation = normalize_dtcc_quote(example_trade_spread)
    logger.info(f"Trade EX001: Basis={basis}, Value={val}, Notation={notation}")

    example_trade_price = pd.Series({
        "dissemination_id": "EX002",
        "price_value": "1.25", "price_notation": "POINTS_UPFRONT", # Hypothetical notation
        "spread_value": None, "spread_notation": None
    })
    basis, val, notation = normalize_dtcc_quote(example_trade_price)
    logger.info(f"Trade EX002: Basis={basis}, Value={val}, Notation={notation}")

    example_trade_decimal_spread = pd.Series({
        "dissemination_id": "EX003",
        "price_value": None, "price_notation": None,
        "spread_value": "0.0065", "spread_notation": "DECIMAL"
    })
    basis, val, notation = normalize_dtcc_quote(example_trade_decimal_spread)
    logger.info(f"Trade EX003: Basis={basis}, Value={val}, Notation={notation}")
