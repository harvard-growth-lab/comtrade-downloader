#!/bin/bash

echo "Starting downloader script..."

today=$(date +'%Y-%m-%d')

# Activate your Python virtual environment if needed
source /n/hausmann_lab/lab/atlas/comtrade_download/venv/bin/activate

python -c "from downloader import ComtradeDownloader"

output_file="/n/hausmann_lab/lab/atlas/comtrade_download/comtrade_script_download_reports/download_output_$today_date.txt"
python /n/hausmann_lab/lab/atlas/comtrade_download/download_latest.py > "$output_file"

