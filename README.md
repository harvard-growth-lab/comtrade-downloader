# Comtrade Downloader

Download and process UN Comtrade bilateral trade data for SITC and HS product classifications systems convert original classification vintages into requested classification vintages. 

## What This Does

Download and archive country reported trade data from UN Comtrade in SITC & HS classification systems. Pulls latest country reported data, based on previous data downloaded. Outputs data aggregated by year for requested classification for ease of analysis, while also storing raw files downloaded by Comtrade. 

### Prerequisites
- Python 3.10 or higher
- [Poetry](https://python-poetry.org/docs/) for managing dependencies
- Premium UN Comtrade API key ([get one here](https://comtradeplus.un.org/))

## Installation

```bash
git clone https://github.com/harvard-growth-lab/comtrade-downloader.git
cd comtrade-downloader
poetry install && poetry shell

# Load Comtrade API key
export COMTRADE_API_KEY="your_key_here"
```

## Quick Start

1. **Configure** which classifications and year ranges in `user_config.py`
2. **Run** `python main.py`
3. **Find results** in your configured output directory

## Configuration

Edit `user_config.py`:

# Set your data directory
```python
OUTPUT_BASE_DIR = "/path/to/your/data/directory"
```
# Classifications to Download
ENABLED_CLASSIFICATIONS = {
    "H5": True,   # Enable HS 2017
    "H6": False,  # Disable HS 2022
    # ... other classifications
}

# Year Range
END_YEAR = None  # Defaults to current year - 1

# Processing Steps
```
PROCESSING_STEPS = {
    "run_downloader": True,  # Download data
    "run_converter": True,   # Convert between classifications
    "run_compactor": True,   # Aggregate data by year
}
```

## Supported Classifications

### Harmonized System (HS)
- **H1**: HS 1992 vintage (1992-present)
- **H2**: HS 2002 vintage (2002-present)
- **H3**: HS 2007 vintage (2007-present)
- **H4**: HS 2012 vintage (2012-present)
- **H5**: HS 2017 vintage (2017-present)
- **H6**: HS 2022 vintage (2022-present)

### Standard International Trade Classification (SITC)
- **S1**: SITC Revision 1 (1962-present)
- **S2**: SITC Revision 2 (1976-present)
- **S3**: SITC Revision 3 (1988-present)
- **S4**: SITC Revision 4 (2007-present)


## How It Works

### 1. Data Download
- Downloads bilateral trade data from UN Comtrade API
- Supports both "classic" (as-reported) and "final" (converted by Comtrade) data
- Handles incremental updates to avoid re-downloading existing data

### 2. Data Conversion (Optional)
- Converts data between different classification systems
- Uses weighted conversion tables for accurate mapping
- Supports complex many-to-many product relationships

### 3. Data Aggregation
- Combines individual country files into annual datasets
- Applies data quality filters and corrections
- Outputs bilateral files

## Output Structure

```
your_data_directory/
├── as_reported/           # Raw data as reported by countries
│   ├── raw/              # Original gzipped files
│   ├── raw_parquet/      # Converted to Parquet format
│   └── converted/        # Classification-converted data
├── by_classification/     # Pre-converted data from Comtrade
│   └── aggregated_by_year/
│       └── parquet/      # Output
├── logs/                 # Processing logs
```

## Usage Examples

### Download HS 2017 data for specific countries
```python
# In user_config.py
ENABLED_CLASSIFICATIONS = {"H5": True}
END_YEAR = 2023
```

## License

Apache License, Version 2.0 - see LICENSE file.

## Citation

```bibtex
@Misc{comtrade-downloader,
  author={Harvard Growth Lab},
  title={Comtrade Downloader},
  year={2025},
  howpublished = {\url{https://github.com/harvard-growth-lab/comtrade-downloader}},
}
```
