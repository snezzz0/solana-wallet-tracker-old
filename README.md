# CopyBot

CopyBot is a sophisticated automated system designed to **track, analyze, and optimize Solana copy-trading wallets**. It monitors specific wallets, alerts on their trades, tracks the performance (PNL) of the tokens they buy, and automatically suggests or implements wallet replacements based on performance data.

## Workflow

### 1. Ingestion & Monitoring (`bots/discordbot.py`)
*   **Trigger:** Listens to a specific Discord channel (fed by Helius webhooks) for transaction alerts.
*   **Processing:** Parses transaction embeds, handling standard swaps and "PUMP_AMM" interactions. It enriches this data by fetching:
    *   Token prices/metadata (Jupiter, DexScreener).
    *   Risk scores (Rugcheck).
    *   Transaction details (Alchemy).
*   **Output:** Sends a formatted alert to a Discord channel and logs the trade to `data/transaction_log.csv`.

### 2. Performance Tracking (`bots/ohlcv_collector.py`)
*   **Trigger:** Continuously monitors `transaction_log.csv` for new "First Holder" buy events.
*   **Action:** Schedules a data fetch (typically 3 hours after the buy).
*   **Analysis:** Queries **Bitquery** for OHLCV (Open/High/Low/Close/Volume) data to calculate the token's performance (Highest PNL, Current PNL).
*   **Output:** Updates `data/token_summaries.csv`, saves detailed CSV logs, and sends a PNL report to Discord.

### 3. Wallet Optimization (`bots/wallet_swap.py`)
*   **Trigger:** Scheduled to run every 48 hours (via `main.py`).
*   **Analysis:**
    *   Identifies inactive wallets or those with poor ROI based on local data.
    *   Queries **Dune Analytics** to find top-performing wallets globally.
    *   Scrapes **GMGN.ai** (using a custom Cloudflare bypass) to validate potential new wallets.
*   **Action:** Generates a recommendation report and can automatically update **Helius** webhooks to swap out underperforming wallets for new ones.

## Tech Stack

*   **Language:** Python 3.x
*   **Orchestration:** `main.py` uses `asyncio` and the `schedule` library to manage concurrent bots.
*   **Database/Storage:** Local CSV files (`transaction_log.csv`, `token_summaries.csv`) and JSON config files.
*   **Browser Automation:** Selenium (likely `undetected-chromedriver`) for scraping GMGN.ai.

## Key Libraries & APIs

| Category | Libraries/APIs | Usage |
| :--- | :--- | :--- |
| **Blockchain Data** | **Helius** | Webhooks for real-time transaction monitoring. |
| | **Alchemy** | Deep transaction parsing (especially for Pump.fun AMM). |
| | **Bitquery** | OHLCV data for PNL calculations. |
| | **Dune Analytics** | Macro-level query to find new profitable wallets. |
| | **Solscan** | Wallet data extraction. |
| **Market Data** | **Jupiter API** | Real-time token pricing (preferred). |
| | **DexScreener** | Fallback pricing and volume data. |
| | **Rugcheck** | Risk analysis for tokens. |
| **Integration** | **Discord.py** | Sending alerts and reports to Discord. |
| | **Telethon** | Forwarding alerts to Telegram (supported in `copybuy_bot`). |
| **Utilities** | `pandas` | Data analysis and CSV manipulation. |
| | `aiohttp`/`requests` | Asynchronous API calls. |
| | `BeautifulSoup` | Parsing HTML from scraped pages. |

## Project Structure

*   `bots/`: Core logic for the three main services.
*   `config/`: YAML and JSON configurations (including wallet lists).
*   `data/`: Stores runtime CSV databases and downloaded logs.
*   `scripts/`: Helper scripts for scraping and data extraction.
