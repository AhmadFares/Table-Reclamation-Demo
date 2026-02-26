# Table Reclamation Demo

## Overview

This repository contains a standalone demo of a **Stats-Guided Analytical Pattern (AP) generation system** for structured data discovery.

The pipeline:

**Natural Language → Structured UR → Stat-Guided SQL Plan → Execution → Pruning**

This demo operates over split versions of the MATHE dataset.

---

## Project Structure

```
demo/
  ui_app.py
  gen_ap.py
  nl_to_ur.py
  execute_ap.py
  prune.py
  metrics.py
  lexicon.json

data/
  MATHE_random_100/
    src_*.csv
    stats.parquet
    value_index.json
```

---

## Setup

### 1. Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Run the Demo

```bash
streamlit run demo/ui_app.py
```

Open the local URL in your browser.

---

## Dataset

- Dataset: MATHE
- Split: random_100
- Sources stored as CSV
- Statistics stored in Parquet and json

Execution runs fully in-memory using DuckDB.

---

## AP Payload (PGJSON)

Each generated Analytical Pattern contains:

- `nl` – original natural language query
- `ur` – structured User Request
- `source_order` – ordered selected sources
- `sql_plan` – executable SQL steps
- `meta` – dataset, split, method, timestamp

---

## Author

Ahmad Fares
2026

---
