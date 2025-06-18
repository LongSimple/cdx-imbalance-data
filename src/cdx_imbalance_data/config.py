# Configuration settings for cdx-imbalance-data

# --- DTCC PPD API Slice Poller Settings (using cloudscraper) ---
DTCC_PPD_API_BASE_URL = "https://pddata.dtcc.com/ppd/api/report/intraday/cftc/"
# Filename example: CFTC_SLICE_CREDITS_2025_06_04_3.zip
# Full URL example: https://pddata.dtcc.com/ppd/api/report/intraday/cftc/CFTC_SLICE_CREDITS_2025_06_04_3.zip

# --- DTCC S3 EOD Cumulative Settings ---
DTCC_S3_EOD_CUMULATIVE_BASE_URL = "https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/"
# Filename example: CFTC_CUMULATIVE_CREDITS_2025_06_03.zip
FETCH_EOD_CUMULATIVE_FOR_LOOKBACK = True # Fetch EOD files for past days in lookback period

DTCC_POLL_ASSET_CLASSES = ["CREDITS"]             # Asset classes to poll (e.g., ["INDEX", "CREDITS"])
DTCC_POLL_INTERVAL_SECONDS = 15                   # Polling frequency for PPD API slices
DTCC_INITIAL_LOOKBACK_DAYS = 1                    # Days to look back (for EOD and/or slices)
DTCC_MAX_SEQ_ATTEMPTS_PER_CYCLE = 20 # Max sequences to try per asset/date for PPD API slices

# --- Cloudscraper Settings ---
CS_BROWSER_VERSION = "139" # Should be a recent Chrome major version
CS_REFERER_URL = "https://pddata.dtcc.com/ppd/cftcdashboard"

# --- File Saving Settings ---
SAVE_DOWNLOADED_FILES = False # Whether to save ZIP and extracted CSV
DOWNLOAD_DIR = "data/dtcc_data" # Base directory for downloads
SLICES_DOWNLOAD_SUBDIR = "ppd_api_slices"
EOD_DOWNLOAD_SUBDIR = "s3_eod_cumulative"

# --- Old GTR Slice Poller Settings (Not used by PPD API method) ---
# DTCC_GTR_BASE_URL = "https://pddata.dtcc.com/gtr"
# DTCC_DEFAULT_JURISDICTION = "cftc"
# DTCC_DEFAULT_REGULATOR_PATH = "sdr"

# --- Original DTCC Settings (Review for new poller relevance) ---
DTCC_PDATA_URL = "https://pddata.dtcc.com" # General base URL
DTCC_FILE_PREFIX_INDEX = "CFTC_INDEX_" # Old prefix
DTCC_CREDITS_SLICE_NAME_PATTERN = r"CFTC_SLICES_CREDITS_.*\.csv" # Old pattern

# Bloomberg Settings
BLOOMBERG_CDX_IG_5Y_TICKER = "CDXIG5 Curncy"
BLOOMBERG_TIMEZONE_OVERRIDE = "UTC"

# Trade Classification Settings
LATENCY_FILTER_SECONDS = 30

# Product Filtering
DTCC_PRODUCT_ID_PATTERN = r"CDX\.NA\.IG\.S\d+"

# File Paths (relative to project root)
DATA_DIR = "data" # General data directory (DOWNLOAD_DIR is now more specific)
DTCC_RAW_DIR = f"{DATA_DIR}/dtcc_raw" # Potentially for other raw data
OUTPUT_CSV_DIR = f"{DATA_DIR}/output_csv"

# Logging
LOG_LEVEL = "DEBUG" # e.g., DEBUG, INFO, WARNING, ERROR

# Economic Conversion
STANDARD_RECOVERY_RATE = 0.4
STANDARD_CDX_IG_COUPON = 0.01

# Add other configurations as needed
TARGET_UPI = 'QZ0PH5HG4P9T'
