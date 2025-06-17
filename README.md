# CDX Imbalance Data

This project provides a comprehensive solution for analyzing Credit Default Swap (CDS) imbalance data. It includes tools for fetching, aggregating, and classifying trade data to identify market trends and imbalances.

## Overview

The primary goal of this project is to process and analyze CDS data from various sources, including the Depository Trust & Clearing Corporation (DTCC) and Bloomberg. By classifying trades and aggregating data, it offers insights into market sentiment and potential trading opportunities.

## Features

- **Data Fetching**: Automatically downloads the latest trade data from the DTCC.
- **Data Aggregation**: Combines and processes raw data into a structured format.
- **Trade Classification**: Classifies trades to determine market direction and sentiment.
- **Economic Conversions**: Provides utilities for converting economic data.
- **Bloomberg Integration**: Connects to Bloomberg for additional data and analysis.

## How to Use

1. **Installation**:
   - Clone the repository:
     ```bash
     git clone https://github.com/LongSimple/cdx-imbalance-data.git
     ```
   - Install the required dependencies:
     ```bash
     pip install -r requirements.txt
     ```

2. **Configuration**:
   - Set up your configuration in `src/cdx_imbalance_data/config.py`. This may include API keys and other settings.

3. **Running the Application**:
   - Execute the main script to start the data analysis process:
     ```bash
     python src/cdx_imbalance_data/main.py
     ```

## Project Structure

- `data/`: Contains raw and processed data files.
- `src/cdx_imbalance_data/`: The main source code for the project.
  - `main.py`: The entry point for the application.
  - `dtcc_fetcher.py`: Handles data fetching from the DTCC.
  - `data_aggregator.py`: Aggregates and processes data.
  - `trade_classifier.py`: Classifies trades based on various criteria.
  - `bloomberg_connector.py`: Manages the connection to Bloomberg.
  - `economic_conversions.py`: Includes functions for economic data conversions.
  - `config.py`: Contains configuration settings.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue to discuss potential changes.
