# Sports Data Engineering Pipeline — Azure Medallion Architecture

An end-to-end Azure data engineering project that ingests sports data from **CSV source files** (Games, Players, Teams), processes them through a **Medallion Architecture (Bronze → Silver → Gold)** using **PySpark on Azure Databricks**, and stores all layers in **Azure Data Lake Storage Gen2**.

---

## Architecture Overview

```
CSV Source Files
(games.csv, players.csv, teams.csv)
        │
        ▼
  Azure Data Factory (sportsfactory)
  [Orchestration & Scheduling]
        │
        ▼
  ADLS Gen2 — sportsproject storage account
  ┌────────────────────────────────────────────┐
  │  source/   ← Raw CSV uploads               │
  │  bronze/   ← Raw Delta tables              │
  │  silver/   ← Cleaned & transformed Delta   │
  │  gold/     ← Aggregated dimension tables   │
  │  meta/     ← Pipeline metadata             │
  └────────────────────────────────────────────┘
        │
        ▼
  Azure Databricks (sports_workspace)
  [PySpark Transformations — notebooks]
        │
        ▼
  Gold Layer — Delta + Parquet
  (Dim_Players, Dim_Players_Parquet)
```

---

## ️ Azure Resources

| Resource | Type | Location | Purpose |
|---|---|---|---|
| `sports_workspace` | Azure Databricks | Central India | PySpark notebook processing |
| `sportsfactory` | Azure Data Factory V2 | Indonesia Central | Pipeline orchestration |
| `sportsproject` | ADLS Gen2 Storage Account | Indonesia Central | All-layer data lake storage |
| `sports_connector` | Access Connector for Databricks | Central India | Managed Identity auth to ADLS |
| Event Grid Topic | Event Grid System Topic | Indonesia Central | Storage event triggers |

---

## ️ Storage Structure (ADLS Gen2 — `sportsproject`)

```
sportsproject/
├── source/                    # Raw CSV file uploads (games, players, teams)
├── bronze/
│   ├── games/                 # Raw games Delta table
│   ├── players/               # Raw players Delta table
│   ├── teams/                 # Raw teams Delta table
│   ├── checkpoint_games/      # Streaming checkpoint — games
│   ├── checkpoint_players/    # Streaming checkpoint — players
│   └── checkpoint_teams/      # Streaming checkpoint — teams
├── silver/
│   ├── games/                 # Cleaned & typed games
│   ├── players/               # Cleaned & typed players
│   ├── teams/                 # Cleaned & typed teams
│   └── regions/               # Derived regions dimension
├── gold/
│   ├── Dim_Players/           # Final player dimension (Delta)
│   └── Dim_Players_Parquet/   # Parquet export for BI tools
└── meta/                      # Pipeline run metadata & watermarks
```

---

## Notebooks / Scripts

| Script | Layer | Description |
|---|---|---|
| `Parameters_Games.py` | Config | Storage account paths, container names, config values |
| `BronzeLayer_Games.py` | Bronze | Reads CSV from `source/`, writes raw Delta to `bronze/` |
| `Silver_Games.py` | Silver | Cleans games data — nulls, types, deduplication |
| `Silver_Players.py` | Silver | Cleans players data — normalizes columns |
| `Silver_Teams.py` | Silver | Cleans teams data |
| `Silver_Regions.py` | Silver | Derives regions dimension from teams/games data |
| `Gold_Games.py` | Gold | Aggregates game-level metrics |
| `Gold_Players.py` | Gold | Builds final `Dim_Players` dimension table |

---

## ️ Tech Stack

| Category | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Orchestration | Azure Data Factory V2 |
| Compute | Azure Databricks (PySpark) |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Export Format | Parquet |
| Language | Python 3 / PySpark |
| Data Source | CSV files (Games, Players, Teams) |

---

## How to Run

### Prerequisites
- Azure subscription with `sports_project` resource group provisioned
- Storage account `sportsproject` with containers: `source`, `bronze`, `silver`, `gold`, `meta`
- Databricks workspace `sports_workspace` with Access Connector linked
- ADF instance `sportsfactory` configured

### Steps

1. **Upload source CSVs** — Place `games.csv`, `players.csv`, and `teams.csv` into the `source/` container in ADLS.

2. **Configure parameters** — Update `Parameters_Games.py` with your storage account name and container paths.

3. **Run Bronze layer** — Execute `BronzeLayer_Games.py` in Databricks — reads CSVs, writes raw Delta tables to `bronze/`.

4. **Run Silver layer** — Run in order: `Silver_Games.py` → `Silver_Players.py` → `Silver_Teams.py` → `Silver_Regions.py`

5. **Run Gold layer** — Execute `Gold_Games.py` then `Gold_Players.py` to produce final dimension tables.

6. **Automate via ADF** — Trigger the full pipeline using the scheduled trigger in `sportsfactory`.

---

## Gold Layer Output

| Table | Format | Description |
|---|---|---|
| `Dim_Players` | Delta | Final player dimension table |
| `Dim_Players_Parquet` | Parquet | Exported for Power BI / downstream tools |

---

## Security Notes

- Databricks authenticates to ADLS using **Azure Managed Identity** via `sports_connector` — no hardcoded keys.
- All containers are set to **Private** access level.
- Never commit storage account keys or SAS tokens — see `.gitignore`.

---

## Author

**Sejal** — Azure Data Engineering Portfolio Project
