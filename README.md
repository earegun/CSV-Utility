# CSV Utility

A powerful CSV processing toolkit for email extraction, expansion, and bulk deduplication.
Built for handling large email datasets and CSV files efficiently.

This tool is designed for email marketing, data cleaning, and large CSV dataset processing.

---

## Features

### 1. Email Expander

Extracts emails from multiple CSV columns and expands them into one email per row.

**What it does:**

* Extract emails from multiple columns
* Expand multiple emails into separate rows
* Deduplicate emails
* Detect verified emails
* Track source column
* Flag duplicate emails
* Download processed CSV

**Supported email columns:**

```
BUSINESS_EMAIL
PERSONAL_EMAILS
PERSONAL_VERIFIED_EMAILS
BUSINESS_VERIFIED_EMAILS
```

---

### 2. Bulk Deduplicator

Deduplicates emails across multiple CSV files.

**How it works:**

1. Upload multiple CSV files
2. Tool scans all files
3. Finds duplicate emails across files
4. Keeps the richest row (most data)
5. Removes duplicates
6. Downloads cleaned files as ZIP

**Requirement:**
Each CSV must contain:

```
PRIMARY_EMAIL
```

---

## Installation

### Clone Repository

```bash
git clone https://github.com/earegun/CSV-Utility.git
cd CSV-Utility
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Application

```bash
streamlit run app.py
```

---

## Usage

### Email Expander Workflow

1. Upload CSV file
2. Click **Process & Expand Emails**
3. Tool extracts and deduplicates emails
4. Download processed CSV

### Bulk Deduplicator Workflow

1. Upload multiple CSV files
2. Tool scans all files
3. Removes duplicates across files
4. Download ZIP file with cleaned CSVs

---

## Output Columns (Email Expander)

The processed CSV will include:

| Column           | Description           |
| ---------------- | --------------------- |
| PRIMARY_EMAIL    | Extracted email       |
| IS_VERIFIED      | Verified email flag   |
| EMAIL_SOURCE     | Source column         |
| DUPLICATE_FLAG   | Duplicate email flag  |
| OCCURRENCE_COUNT | Number of occurrences |

---

## Performance

Designed for:

* Large CSV files
* Millions of emails
* Bulk deduplication
* Memory-efficient processing
* Chunked CSV reading

---

## Tech Stack

* Python
* Streamlit
* Pandas
* Regex
* Zipfile

---

## Project Structure

```
CSV-Utility/
│
├── app.py
├── requirements.txt
├── README.md
└── screenshots/
```

---

## Use Cases

* Email marketing list cleaning
* Email extraction from CSV
* CSV deduplication
* Lead database cleaning
* Data normalization
* Bulk email dataset processing

---

## License

MIT License

---

## Author

GitHub: https://github.com/earegun
