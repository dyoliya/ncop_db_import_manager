# NCOP DB Import Manager

**NCOP DB Import Manager** is a desktop GUI utility that imports NCOP data from CSV/Excel files, cleans and normalizes records, and appends them to a SQLite database with schema checks and daily database versioning.

---

![Version](https://img.shields.io/badge/version-1.0.1-ffab4c?style=for-the-badge&logo=python&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11%2B-273946?style=for-the-badge&logo=python&logoColor=ffab4c)
![Status](https://img.shields.io/badge/status-active-273946?style=for-the-badge&logo=github&logoColor=ffab4c)

---

## 🚧 Problem Statement / Motivation
The **Heirship No Contact Operations (NCOP)** dataset is maintained in a multi-tab Google Sheet that serves as a working document for several internal teams. While the spreadsheet enables collaboration, it also introduces operational challenges:

- The document contains multiple working tabs and is actively edited, increasing the risk of accidental modifications.
- Several users need to frequently reference the **“Consolidated List”** tab, which can lead to performance slowdowns when accessed simultaneously.
- Google Sheets does not provide a stable, read-only data snapshot suitable for database-style queries or structured data access.

To address this, the **NCOP DB Import Manager** was created to convert exported **Consolidated List** data into a **local SQLite database** (`.db`). The tool allows users to import CSV or Excel exports of the Consolidated List and append them to a managed database.

Using a SQLite database provides:
- a **stable, read-only version** of the dataset that prevents accidental edits,
- **faster querying and navigation** compared to large spreadsheets, and
- a controlled mechanism to **append new records over time** as the Consolidated List grows.

The tool ensures that incoming data follows a consistent schema, preserves historical database versions, and provides a reliable local data source for operational use.

---

## ✨ Features
- Desktop GUI built for selecting and importing one or multiple input files.
- Supports CSV and Excel-based formats (`.csv`, `.xlsx`, `.xlsm`, `.xls`).
- Daily SQLite DB naming and rollover under a local `database/` directory.
- Strict schema validation for existing tables (detects missing/unexpected columns).
- Data repair helpers for known spreadsheet edge cases:
  - phone values displayed as hashes (`#######`),
  - extraction of true hyperlink targets,
  - ownership fraction recovery (e.g., `1/2`, `1/3`) when Excel interprets as dates.
- Activity log and progress feedback during import.

---

## 🧠 Logic Flow
1. User selects one or more input files from the app.
2. The app computes today’s DB path (copy-forward from latest DB if needed).
3. Each file is loaded and normalized (CSV/Excel branch logic).
4. Transform/cleaning utilities prepare DataFrame columns and records.
5. The incoming file schema is validated against the existing target table.  
   - If the database already exists, the input columns **must match the current schema**.
   - If there are **missing or unexpected columns**, the import process stops and an error is raised.
6. Rows are inserted into SQLite, and upload metadata is added.
7. App logs status and completion results in the UI.

---

## 📝 Requirements
- Python 3.10+ (recommended)
- OS with GUI support (Windows/macOS/Linux desktop session)
- Python packages listed in `requirements.txt`

---

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
4. **Folder Structure**

      <pre>project/
      │
      ├── app.py                                Main application entry point (GUI + import workflow)
      ├── audit/                                Generated audit logs for NULL conversions or data issues
      ├── ncop_sqlite.py                        SQLite database utilities (connect, schema management, inserts)
      ├── ncop_transform.py                     Data cleaning and transformation logic before DB insertion
      └── database/                             Local SQLite database storage
          ├── yyyy-mm-dd-ncop.db                Active daily database version
          └── previous_versions/                Archived older database versions
    </pre>
    
    
5. Run the application:
   ```bash
   python app.py
   ```
---

## 🖥️ User Guide
1. Launch the app with `python app.py`.
2. Click **Browse** and choose one or multiple source files.
3. Review selected files in the file list panel.
4. Click **Import & Append to DB**.
5. Monitor the **Activity Log** and progress bar.
6. Find generated DB files in the local `database/` folder.

---

## 👩‍💻 Credits
- **2026-03-03**: Project created by **Julia** ([@dyoliya](https://github.com/dyoliya))  
- 2026–present: Maintained by **Julia** for **Community Minerals II, LLC**
