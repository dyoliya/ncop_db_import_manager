# NCOP DB Import Manager

## Brief Description
NCOP DB Import Manager is a desktop GUI utility that imports NCOP data from CSV/Excel files, cleans and normalizes records, and appends them to a SQLite database with schema checks and daily database versioning.

## 🚧 Problem Statement / Motivation
Manually consolidating land/customer datasets from mixed spreadsheet formats can be slow and error-prone. Teams often face:
- inconsistent column naming and formatting,
- Excel-specific value issues (e.g., `#######` overflow display, hyperlink cells, fraction/date confusion), and
- difficulty preserving a trusted historical database while continuing daily imports.

This tool centralizes those workflows into one import experience with validation and repeatable data handling.

## ✨ Features
- Desktop GUI built with `customtkinter` for selecting and importing one or multiple input files.
- Supports CSV and Excel-based formats (`.csv`, `.xlsx`, `.xlsm`, `.xls`).
- Daily SQLite DB naming and rollover under a local `database/` directory.
- Strict schema validation for existing tables (detects missing/unexpected columns).
- Data repair helpers for known spreadsheet edge cases:
  - phone values displayed as hashes (`#######`),
  - extraction of true hyperlink targets,
  - ownership fraction recovery (e.g., `1/2`, `1/3`) when Excel interprets as dates.
- Activity log and progress feedback during import.

## 🧠 Logic Flow
1. User selects one or more input files from the app.
2. The app computes today’s DB path (copy-forward from latest DB if needed).
3. Each file is loaded and normalized (CSV/Excel branch logic).
4. Transform/cleaning utilities prepare DataFrame columns and records.
5. Schema is checked against the existing target table.
6. Rows are inserted into SQLite, and upload metadata is added.
7. App logs status and completion results in the UI.

## 📝 Requirements
- Python 3.10+ (recommended)
- OS with GUI support (Windows/macOS/Linux desktop session)
- Python packages listed in `requirements.txt`

## 🚀 Installation and Setup
1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd ncop_db_import_manager
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate    # Linux/macOS
   # .venv\Scripts\activate     # Windows PowerShell/CMD
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the application:
   ```bash
   python app.py
   ```

## 🖥️ User Guide
1. Launch the app with `python app.py`.
2. Click **Browse** and choose one or multiple source files.
3. Review selected files in the file list panel.
4. Click **Import & Append to DB**.
5. Monitor the **Activity Log** and progress bar.
6. Find generated DB files in the local `database/` folder.

## 👩‍💻 Credits
- Developed by **dyoliya**.
- UI and workflow tailored for NCOP import operations.
