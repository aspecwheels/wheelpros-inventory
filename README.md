# WheelPros Inventory Downloader

Downloads and imports the daily WheelPros inventory feed from email, uploads key columns to Google Sheets, and logs each import.

## Features

- Downloads ZIP from secure email link
- Extracts CSV and uploads to Sheet1
- Logs every import (with change from previous day)
- Automatically archives and marks email as read

## Usage

- Requires Google Cloud OAuth credentials (`credentials.json`)
- Do NOT commit your `credentials.json` or `token.json` (see .gitignore)
