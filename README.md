# 🎯 Zone-Based Footprint & Macro Validation Agent

> **Institutional-grade order flow sniper and macroeconomic disaster shield engineered to run 24/7 on legacy potato hardware with zero GPU, zero ML bloat, and ~0% idle CPU.**

[![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue.svg?style=flat-square)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)
[![Dependencies](https://img.shields.io/badge/Dependencies-aiohttp%20only-orange.svg?style=flat-square)](#-hardware-democratization)
[![Memory Footprint](https://img.shields.io/badge/RAM%20Footprint-%3C%2050MB-success.svg?style=flat-square)](#-performance-benchmarks)
[![Engine](https://img.shields.io/badge/Complexity-O(1)%20Circular-red.svg?style=flat-square)](#-micro-engine-mechanics)

---

## ⚡ Overview

Most retail quantitative tools waste gigabytes of RAM computing lagging moving averages across historical candles.

**ZoneSniper** takes the opposite approach:
1. **Sleeps at near 0% CPU** until market price penetrates a user-defined ambush zone.
2. **Awakens instantly** to process Binance raw trades (`@aggTrade`) and L2 order book snapshots (`@depth20@100ms`).
3. **Calculates Cumulative Volume Delta (CVD)** and detects institutional iceberg absorption via an $O(1)$ ring buffer.
4. **Validates macro conditions** against an asynchronous, locally cached Federal Reserve (FRED) economic risk feed before dispatching precision Telegram alerts.

---

## 🏛️ System State Architecture

```text
 ┌─────────────────────────────────────────────────────────────┐
 │                      SLEEPING STATE                         │
 │   - Lightweight REST polling every 5s (< 1KB network)       │
 │   - CPU: ~0.0% | FRED Macro Shield updating in background   │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Price touches [Lower, Upper]
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                      SNIPER ACTIVE                          │
 │   - Direct WebSocket: @aggTrade + @depth20@100ms            │
 │   - O(1) Rolling CVD Accumulator (15s sliding window)       │
 │   - Real-time Bid/Ask Wall Absorption Analysis              │
 └──────────────┬───────────────────────────────┬──────────────┘
                │ Micro Exhaustion Confirmed    │ Price breaches bounds
                │ + Macro Risk Check Cleared    │ + Hysteresis (0.25%)
                ▼                               ▼
 ┌──────────────────────────────┐ ┌────────────────────────────┐
 │      INSTANT DISPATCH        │ │      COOLDOWN STATE        │
 │ - Push Telegram MarkdownV2   │ │ - 30s Debounce Watchdog    │
 │ - SQLite WAL Audit Log       │ │ - Smooth fallback to sleep │
 │ - 10s Rate-Limit Cooldown    │ │   if price stays outside   │
 └──────────────────────────────┘ └────────────────────────────┘
```

---

## 💎 Core Pillars

### 1. Hardware Democratization
* **Tailored for Legacy PCs**: Runs effortlessly on dual-core/quad-core office rigs (e.g., Intel Core i7-4770, 8GB DDR3 RAM, Windows 10/11 or headless Linux).
* **Zero Heavy Bloat**: **No Pandas. No NumPy. No PyTorch.**
* **Pure Python YAML Parser**: Built-in stack-based parser completely eliminates `pyyaml` and Microsoft Visual C++ 14.0 build dependencies.
* **$O(1)$ Memory Footprint**: Strictly maintains ring buffers using `collections.deque(maxlen=4096)` and `__slots__` dataclasses. Maximum RAM usage is capped under 50MB.

### 2. Zero-Lag Microstructure Engine
Ditches lagging OHLCV candles to read raw limit-to-market interactions:
* **Incremental CVD Calculation**: Maintains rolling Delta accumulators with arithmetic additions/subtractions upon ingestion and eviction—zero loop recalculations.
* **Absorption / Exhaustion Detection**: 
  Identifies situations where market dumping is absorbed by iceberg limit bids based on this core condition:
  `Imbalance = (Taker Sell / Taker Buy) >= 2.5` **AND** `(|ΔP| / P) <= 0.08%` **AND** `Bid Wall Depth >= 2.0 × Taker Sell`

### 3. FRED Macro Disaster Shield
An independent, background-updated local cache tracks systemic solvency risks via the St. Louis Fed API without stalling the event loop:
* **Liquidity Inversion**: Overnight Reverse Repo (`RRPONTSYD`) drain monitoring.
* **Recession Warning**: 10Y-3M Treasury Yield Spread (`T10Y3M`) un-inversion triggers.
* **Credit Contagion**: ICE BofA US High Yield Index Option-Adjusted Spread (`BAMLH0A0HYM2`) blowouts ($> 450\text{ bps}$).
* **Employment Shock**: Real-time Sahm Rule Recession Indicator (`SAHMCURRENT` $\ge 0.50\%$).

### 4. Industrial Defensive Design
* **Deadband & Hysteresis**: 0.25% buffer bands prevent reconnection storms when price hovers near zone edges.
* **Stall Watchdog**: Automatically kills and rebuilds sockets if Binance drops data for $> 15$ seconds.
* **Full-Jitter Exponential Backoff**: Prevents IP rate-limit bans during exchange disconnects.
* **SQLite WAL Engine**: Persistent event logging with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL` for lock-free concurrency.
* **MarkdownV2 Sanitizer**: Regex-based token-escaping prevents Telegram API 400 Bad Request drops.

---

## 📊 Performance Benchmarks

Tested on a benchmark legacy rig (**Intel Core i7-4770 @ 3.40GHz, 8GB RAM, Windows 10 Pro**):

| Metric | SLEEPING Mode | ACTIVE Sniper Mode |
| :--- | :--- | :--- |
| **CPU Usage** | **0.0% – 0.1%** | **0.8% – 1.4%** |
| **RAM Consumption** | `< 28 MB` | `< 45 MB` (Fixed peak) |
| **Network Overhead** | ~1 KB / 5 sec | ~15–35 KB/s (Compressed WS) |
| **GC Pauses** | 0 ms | `< 1.2 ms` (No heap thrashing) |
| **External Dependencies** | `aiohttp` only | `aiohttp` only |

---

## 🚀 Quick Start

### 1. Clone & Install
```bash
git clone https://github.com/yourusername/zone-footprint-macro-agent.git
cd zone-footprint-macro-agent
pip install aiohttp
```

### 2. Initialize Configuration
Run the script once to generate the default configuration template:
```bash
python agent.py
```

### 3. Edit `config.yaml`
```yaml
target:
  symbol: "BTCUSDT"                  # Pair symbol
  is_futures: true                   # USDT-M Futures stream
  lower_bound: 75000.0               # Ambush zone lower edge
  upper_bound: 76300.0               # Ambush zone upper edge
  hysteresis_pct: 0.0025             # 0.25% anti-thrashing deadband
  cooldown_seconds: 30               # Post-exit cooling observation window
  sleep_poll_interval_sec: 5.0       # REST ping interval while sleeping

micro_engine:
  window_seconds: 15                 # Rolling evaluation window
  min_volume_threshold: 50.0         # Minimum volume to trigger (BTC)
  cvd_imbalance_ratio: 2.5           # Order flow ratio (Sell vs Buy)
  price_impact_tolerance_pct: 0.0008 # Price movement boundary (0.08%)
  depth_absorption_ratio: 2.0        # Limit wall thickness ratio

macro_shield:
  enabled: true
  api_key: "YOUR_FRED_API_KEY"       # Free key from https://fred.stlouisfed.org/
  refresh_interval_hours: 12

notification:
  telegram_bot_token: "YOUR_BOT_TOKEN"
  telegram_chat_id: "YOUR_CHAT_ID"

storage:
  db_path: "data/sniper_records.db"
```

### 4. Run
```bash
# Direct run
python agent.py

# Or via Windows double-click runner
start.bat
```

---

## 📱 Telegram Alert Sample

```text
🟢 【伏击圈信号：卖盘竭尽 + 冰山吸收】

🎯 标的代号: BTCUSDT
📍 当前现价: 75120.50
📐 伏击区间: [75000.00 ~ 76300.00]
⏱️ 监控窗口: 15s
📊 窗口总成交: 142.35
📉 净 CVD 变动: -84.20
📌 微观价格偏移: +0.021%
🧱 盘口防御厚度: 185.40

💡 微观结构判断:
市价猛烈砸盘遭多头主力冰山限价单全部拦截，价格锁死不跌，多头防线极度坚固。

🛡️ FRED 宏观护盾状态: ✅ SAFE
• 隔夜逆回购: $284.1B
• 10Y-3M 利差: -0.18%
• 高收益债利差: 3.22%
• 衰退风险提示: 宏观水温适宜，系统性违约概率偏低

⏰ 时间戳: 2026-11-08 14:15:22
```

---

## 📂 Repository Layout

```text
.
├── agent.py                 # Core unified async engine (zero-dependency runner)
├── start.bat                # Windows escape-proof launcher
├── config.example.yaml      # Reference configuration template
├── README.md                # System documentation
└── data/                    # Generated directory
    └── sniper_records.db    # SQLite database (WAL mode)
```

---

## 🛡️ Disclaimer

```text
This software is provided strictly for educational, structural analysis, and research 
purposes under the MIT License. It does NOT constitute financial, investment, or trading 
advice. The authors accept zero liability for financial losses, exchange API changes, 
connectivity failures, or execution anomalies.
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
