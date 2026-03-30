# 🏭 OEE Analytics Platform with AI-Powered Dashboard

**A complete manufacturing analytics solution** combining:
- 🔄 **Automated ETL Pipeline** (schema detection, data transformation, OEE calculation)
- 📊 **Real-Time Dashboard** (6 KPI metrics, 10-second auto-refresh)
- 🤖 **AI-Powered Q&A** (natural language questions → automatic SQL generation + insights)
- ⚡ **Exasol Database** (in-memory analytics for sub-second queries)

Perfect for **manufacturing facilities** looking to analyze equipment effectiveness, identify downtime patterns, and make data-driven decisions in real-time.

## 📋 How It Works (3-Min Overview)

### **1️⃣ Data Pipeline** (Runs once at startup)
```
Excel Files (plants P01-P12) 
    ↓
[Schema Detection] - Identifies "production", "kpi", "event" types
    ↓
[Mapping] - Creates field-to-column mappings
    ↓
[Transformation] - Calculates OEE = Availability × Performance × Quality
    ↓
[Load] - Inserts normalized data into Exasol (in-memory DB)
    ↓
✅ Ready for queries!
```

### **2️⃣ Dashboard** (Real-time analytics)
```
Exasol Database
    ↓
📊 6 KPI Cards (auto-refresh every 10s)
    • OEE Normalized (%)
    • Availability (%)
    • Performance (%)
    • Quality (%)
    • Total Production (units)
    • Downtime Impact
    ↓
📈 3 Interactive Charts
    • OEE Trend (line chart)
    • Top Plants (bar chart)
    • Downtime Heatmap (calendar)
```

### **3️⃣ AI-Powered Ask Data** (LLM-assisted Q&A)
```
User: "Show OEE trend by plant over time"
    ↓
[Ollama LLM] - Generates SQL from natural language
    ↓
[SQL Validator] - Fixes PostgreSQL → Exasol dialect
    (ILIKE → LIKE, NOW() → CURRENT_TIMESTAMP, etc.)
    ↓
[Query Executor] - Runs validated SQL on Exasol
    ↓
[Chart Generator] - Auto-creates appropriate visualization
    ↓
[LLM Explanation] - Generates 60-70 word business insight
    ↓
✅ Dialog shows: Chart + Explanation + Data + SQL
```

---

## 📋 What's Included

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │   Excel      │  │  Synthetic   │  │  Future: APIs, DBs   │   │
│  │   Files      │  │  JSON Streams│  │                      │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓↓↓
┌─────────────────────────────────────────────────────────────────┐
│              PYTHON ETL PIPELINE (agent/)                        │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐     │
│  │  Schema      │ │  Mapping     │ │  Synthetic Data      │     │
│  │  Detection   │ │  Agent       │ │  Generator           │     │
│  └──────────────┘ └──────────────┘ └──────────────────────┘     │
│         ↓ (JSON schema) ↓ (mapping)   ↓ (generate .jsonl)        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Transform & Load Agent                                  │   │
│  │  • Normalize all schemas to unified format               │   │
│  │  • Calculate OEE = Availability × Performance × Quality  │   │
│  │  • Load to Exasol database                               │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓↓↓
┌─────────────────────────────────────────────────────────────────┐
│         EXASOL DATABASE (In-Memory Analytics)                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  OEE_UNIFIED Table (12 columns)                          │   │
│  │  • plant_id, date, planned_time_min, run_time_min        │   │
│  │  • availability, performance, quality, oee_normalized    │   │
│  │  • 1000s of rows (~12 plants × 30+ days)                │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓↓↓
┌─────────────────────────────────────────────────────────────────┐
│        STREAMLIT DASHBOARD (dashboard/streamlit_app.py)         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Real-Time KPI Cards (6 metrics, 10s auto-refresh)       │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │  OEE Trend Chart | Top Plants Bar | Downtime Heat │  │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │  🤖 Quick Ask Panel (AI-Powered Q&A)              │  │   │
│  │  │  • Natural language questions                      │  │   │
│  │  │  • Auto-generates SQL via Cloud Ollama            │  │   │
│  │  │  • Interactive charts with explanations            │  │   │
│  │  │  • Side-by-side: Chart + Data Insights            │  │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  │  ┌────────────────────────────────────────────────────┐  │   │
│  │  │  Data Quality Diagnostics                          │  │   │
│  │  │  • Schema validation                               │  │   │
│  │  │  • Data anomaly detection                          │  │   │
│  │  └────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📦 Project Structure

```
hackathon-proj/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .env                               # Configuration (credentials, URLs)
├── .env.example                       # Environment template
│
├── 📚 DOCUMENTATION FILES
│   ├── PIPELINE_ARCHITECTURE.md       # Detailed pipeline architecture
│   ├── SQL_VALIDATION.md              # SQL dialect validation & auto-fix
│   ├── IMPLEMENTATION_SUMMARY.md      # Latest feature implementation
│   └── CODE_CHANGES_REFERENCE.md      # Detailed code changes
│
├── agent/                             # ETL Pipeline
│   ├── run_pipeline_synthetic.sh      # Generate synthetic data + load
│   ├── run_pipeline_files.sh          # Load Excel files
│   ├── run_pipeline.sh                # Original combined script
│   ├── schema_detection_agent.py      # Detect file schemas (production, kpi, event, etc.)
│   ├── mapping_agent.py               # Create file-to-schema mapping
│   ├── synthetic_data_generator.py    # Generate test data streams (.jsonl)
│   ├── transform_load_agent.py        # Transform & load to Exasol
│   ├── query_agent.py                 # Sample queries to Exasol
│   ├── mapping_output.json            # Generated schema mapping
│   │└── schema_output.json             # Generated schema detection
│   └── PIPELINE_ARCHITECTURE.md       # Detailed pipeline docs
│
├── dashboard/                         # Streamlit Analytics UI
│   ├── streamlit_app.py               # Main dashboard (OEE analytics + Ask Data)
│   ├── change_detector.py             # Monitors for data changes (auto-refresh)
│   ├── async_query_worker.py          # Background query executor (non-blocking)
│   └── ask_data_audit.csv             # Sample Q&A history
│
├── data/                              # Input data folder
│   ├── plant_P01.xlsx                 # Sample production data
│   ├── plant_P02.xlsx                 # Sample KPI metrics
│   └── ... (more plants)
│
├── synthetic_data/                    # Generated synthetic data (created at runtime)
│   ├── plant_P01.xlsx.jsonl           # Streaming JSON format
│   ├── plant_P02.xlsx.jsonl
│   └── ...
│
├── scripts/                           # Utility scripts
│   ├── mes_connector_cli.py
│   ├── sample_workflow.py
│   └── sample_*.json
│
└── .streamlit/                        # Streamlit configuration
    └── config.toml                    # Theme and UI settings
```

---

---

## 🚀 Quick Start (5 Minutes)

### **Prerequisites**
- Python 3.10+
- Exasol (running on `127.0.0.1:8563`)
- Ollama cloud API key (for Ask Data feature)

### **Setup (One Time)**

```bash
# 1. Navigate to project
cd /home/abinand/hackathon-proj

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
nano .env
# Fill in: EXASOL_* settings, OLLAMA_* settings

# 5. Initialize database (one time)
cd agent/
python3 schema_detection_agent.py
python3 mapping_agent.py
python3 transform_load_agent.py

cd ../
```

### **Run Dashboard**

```bash
# Start Streamlit dashboard
streamlit run dashboard/streamlit_app.py --server.address=0.0.0.0

# Access at: http://localhost:8501
```

---

## 📊 Data Loading & Refresh

### **Load Excel Data (Production Data)**

```bash
cd agent/
./run_pipeline_files.sh

# What it does:
# 1. Scans /data/*.xlsx files
# 2. Detects schema (production, kpi, event types)
# 3. Maps columns to unified format
# 4. Inserts into Exasol OEE_UNIFIED table
# 5. Dashboard auto-refreshes with new data
```

### **Generate Test Data (Synthetic)**

For testing without real factory data:

```bash
cd agent/

# Generate 10 batches, 50 rows each
SYNTHETIC_BATCHES=10 \
SYNTHETIC_ROWS_PER_FILE=50 \
SYNTHETIC_INTERVAL_SEC=0 \
./run_pipeline_synthetic.sh

# View generated files
ls -lh synthetic_data/
```

**Output example:**
```
synthetic_data/
├── plant_P01.xlsx.jsonl (50 rows - production events)
├── plant_P02.xlsx.jsonl (50 rows - KPI metrics)
├── plant_P03.xlsx.jsonl (50 rows - equipment events)
└── ... (12 plants total)
```

### **Auto-Refresh Mode (Continuous Updates)**

For continuous data generation (simulates real factory streaming):

```bash
cd agent/

# Generates 1 batch every 3 seconds indefinitely
SYNTHETIC_BATCHES=0 \
SYNTHETIC_ROWS_PER_FILE=50 \
SYNTHETIC_INTERVAL_SEC=3 \
./run_pipeline_synthetic.sh

# Stop with: Ctrl+C
# Dashboard will continuously refresh with new data
```

---

## 💬 Using Ask Data (AI-Powered Q&A)

Once dashboard is running:

### **Method 1: Type Your Question**

1. Navigate to **"🤖 Quick Ask"** panel (right sidebar)
2. Type a question: `"Show OEE trend by plant over time"`
3. Press **Enter** or click **🔍 Ask**
4. ✨ Dialog opens showing:
   - 📈 Chart (auto-generated visualization)
   - 📖 Explanation (60-70 word business insight)
   - 📋 Data (underlying query results)
   - 📝 SQL (generated & validated query)

### **Method 2: Use Example Questions**

1. Click **📌 Examples** button
2. Select from pre-built questions:
   - "Show OEE trend"
   - "Top 5 plants by OEE"
   - "OEE vs calculated"
   - "Production units trend"
   - "Compare downtime across plants"

### **Example Questions That Work**

```
📊 Trend Analysis:
"Show OEE trend over time"
"Production units by day"
"Downtime impact trend"

🏆 Comparative:
"Top 5 plants by OEE"
"Compare availability across plants"
"Which plant has best quality?"

🔍 Deep Dive:
"OEE vs calculated comparison"
"Planned vs actual production"
"Availability vs performance correlation"

📈 Metrics:
"Average OEE by plant"
"Total downtime by plant"
"Quality score distribution"
```

---

## 🎛️ Dashboard Walkthrough

### **Top Bar (Navigation)**
```
🏭 OEE Analytics Platform | Data Source: Exasol | Refresh: 10s | 📊 Export
```

### **Main Content (Left)**

#### **1. KPI Cards (6 Metrics)**
```
┌─────────────────────────────────────────┐
│  OEE Normalized: 72.3%     ↗ +2.1%      │
│  Availability: 81.2%                    │
│  Performance: 88.9%                     │
│  Quality: 96.1%                         │
│  Production: 1,245 units                │
│  Downtime Impact: 4.3 hrs               │
└─────────────────────────────────────────┘
```

#### **2. 3 Interactive Charts**
- **OEE Trend** (line chart) - Shows OEE changes over time
- **Top Plants** (bar chart) - Plants ranked by OEE
- **Downtime Heatmap** (calendar) - Visual pattern of downtime

### **Right Sidebar (Quick Ask Panel)**
```
💬 Ask a question about your data
┌─────────────────────────────────┐
│ [text input field]              │
│ Example: "OEE by plant"         │
│ [🔍 Ask] [📌 Examples]          │
└─────────────────────────────────┘
```

### **Refresh Behavior**
- ✅ Auto-refreshes every 10 seconds
- ✅ Syncs with database changes
- ✅ Charts animate smoothly
- ✅ No manual refresh needed

---

## 🛠️ Configuration

### **Environment Variables (.env)**

```bash
# Database Connection
EXASOL_SCHEMA=HACKATHON
EXASOL_TABLE=OEE_UNIFIED
EXASOL_HOST=127.0.0.1
EXASOL_PORT=8563
EXASOL_USER=sys
EXASOL_PASSWORD=exasol

# AI/LLM (for Ask Data)
OLLAMA_BASE_URL=https://api.ollama.com
OLLAMA_API_KEY=your-api-key-here
OLLAMA_MODEL=qwen3-coder-next
OLLAMA_TIMEOUT_SEC=45

# Dashboard Settings
ANALYTICS_REFRESH_INTERVAL_SEC=10
ANALYTICS_AUTO_REFRESH=true
ANALYTICS_CACHE_TTL_SEC=300

# Debug
LOG_LEVEL=INFO
DEBUG_MODE=false
```

---

## 📁 Project Structure

View generated data:

```bash
head -5 synthetic_data/plant_P01.xlsx.jsonl
# Output: 5 JSON records
```

---

## 📈 Running the Dashboard

**Start the analytics UI:**

```bash
cd dashboard/
streamlit run streamlit_app.py
```

Opens browser at `http://localhost:8501`

### **Dashboard Features**

**🎯 Real-Time KPI Cards (Top)**
- Active Plants, Avg OEE, Yield Rate
- Avg Downtime, Availability, Performance
- Auto-refreshes every 10 seconds

**📊 Charts (Middle)**
- OEE Trend (line chart over time)
- Top 5 Plants by OEE (bar chart)
- Comparison explorers with filters

**🤖 Quick Ask Panel (Right Side)**
- Type: "Show OEE by plant" or "Compare downtime"
- AI generates SQL via Ollama
- Side-by-side: Chart + Explanation
- Multiple questions supported

**🔧 Data Quality Diagnostics (Bottom)**
- Missing field severity
- Schema completeness
- Recommended fixes

### **Example Questions:**

```
"Show OEE trend"
"Top 5 plants by OEE"
"Compare downtime across plants"
"Production units trend"
"OEE vs calculated"
```

---

## 📝 Data Schemas

The pipeline supports 5 different MES data schemas:

### **1. Production**
Raw production metrics from shop floor systems
```json
{
  "plant_id": "P01",
  "date": "2026-03-25",
  "planned_hours": 10.5,
  "run_hours": 8.2,
  "units_produced": 4500,
  "units_defective": 89
}
```

### **2. KPI**
Pre-calculated OEE component metrics
```json
{
  "plant_id": "P01",
  "date": "2026-03-25",
  "availability_pct": 78,
  "performance_pct": 89.5,
  "quality_pct": 98.2
}
```

### **3. Event**
Equipment events (run/idle/stop)
```json
{
  "plant_id": "P01",
  "date": "2026-03-25",
  "event_type": "RUN",
  "machine": "M-05",
  "duration_sec": 3600
}
```

### **4. Time**
Operating hours and OEE values
```json
{
  "plant_id": "P01",
  "date": "2026-03-25",
  "total_hours": 9.5,
  "operating_hours": 7.8,
  "oee": 0.67
}
```

### **5. Quality**
Quality and downtime statistics
```json
{
  "plant_id": "P01",
  "date": "2026-03-25",
  "total_units": 5000,
  "defective_units": 150,
  "downtime": 45.5,
  "reason": "maintenance"
}
```

---

## 🔄 Pipeline Stages

### **Stage 1: Schema Detection** (`schema_detection_agent.py`)
- Scans `/data/` directory
- Detects schema type (production, kpi, event, time, quality)
- Outputs: `schema_output.json`

### **Stage 2: Mapping** (`mapping_agent.py`)
- Creates file-to-schema mappings
- Outputs: `mapping_output.json`
- Example:
  ```json
  [
    {"file": "plant_P01.xlsx", "schema": "production"},
    {"file": "plant_P02.xlsx", "schema": "kpi"}
  ]
  ```

### **Stage 3: Synthetic Data (Optional)** (`synthetic_data_generator.py`)
- Generates test data matching detected schemas
- Outputs: `/synthetic_data/*.jsonl` (streaming JSON)
- Each line is one record

### **Stage 4: Transform & Load** (`transform_load_agent.py`)
- Reads from `/data/` (Excel) or `/synthetic_data/` (JSONL)
- Normalizes all schemas to unified format
- Calculates: `OEE = Availability × Performance × Quality`
- Loads to Exasol `OEE_UNIFIED` table

### **Stage 5: Query** (`query_agent.py`)
- Runs sample queries against loaded data
- Validates data integrity

---

## 🤖 AI Features (Ask Data)

The dashboard includes natural language SQL generation via **Cloud Ollama**.

### **Setup**

1. Get Ollama API key from https://ollama.com
2. Add to `.env`:
   ```bash
   OLLAMA_API_KEY=your-key
   OLLAMA_BASE_URL=https://ollama.com
   OLLAMA_MODEL=qwen3-coder-next
   ```

### **How It Works**

```
User Question
    ↓
Generate SQL Prompt (schema, available columns)
    ↓
Call Ollama API (Cloud LLM)
    ↓
Extract SQL + Chart Metadata
    ↓
Execute Against Exasol
    ↓
Render Chart + Generate Explanation
```

### **Features**

- ✅ **Structured Output**: SQL + chart type (line, bar, etc.)
- ✅ **Fallback Logic**: Ollama → Heuristic SQL → Local pandas
- ✅ **Explanations**: Data-driven insights (no LLM hallucination)
- ✅ **Interactive**: Unlimited questions, no rate limits
- ✅ **Side-by-Side**: Chart on left, explanation on right
- ✅ **SQL Validation**: Automatic Exasol dialect translation & validation
  - Fixes incompatible functions (ILIKE → LIKE, NOW() → CURRENT_TIMESTAMP, etc.)
  - Validates GROUP BY requirements with aggregates
  - Ensures query safety with read-only checks
  - See [SQL_VALIDATION.md](SQL_VALIDATION.md) for details

---

## ⚡ Real-Time Features

### **Auto-Refresh (10s Interval)**

Dashboard automatically refreshes KPI cards and charts:

```python
ANALYTICS_REFRESH_INTERVAL_SEC=10  # Check every 10 seconds
ANALYTICS_AUTO_REFRESH=true        # Enabled by default
```

### **How It Works**

1. Polls Exasol for row count changes
2. Compares max timestamp to detect new data
3. If changes detected → refreshes dashboard
4. Disabled during "Ask Data" dialog to prevent closing

### **Caching**

```python
ANALYTICS_CACHE_TTL_SEC=300  # Cache results for 5 minutes
```

Prevents excessive database hits while keeping data fresh.

---

## 🧪 Testing & Troubleshooting

### **Test the Full Pipeline**

```bash
# 1. Generate synthetic data
cd agent/
SYNTHETIC_BATCHES=1 SYNTHETIC_ROWS_PER_FILE=5 python3 synthetic_data_generator.py

# 2. Transform & load
PIPELINE_DATA_MODE=synthetic python3 transform_load_agent.py

# 3. Query to verify
python3 query_agent.py
```

### **Common Issues**

**Q: Exasol connection failing**
```
Error: Failed to connect to Exasol after retries
Solution:
  1. Check: exasol-nano status
  2. Wait: It takes 30-60s to start
  3. Verify: netstat -an | grep 8563
```

**Q: Ollama API errors in Dashboard**
```
Error: LLM unavailable (HTTP 500)
Solution:
  1. Verify API key in .env
  2. Check internet connection
  3. Test: curl -H "Authorization: Bearer KEY" https://ollama.com/api/chat
  4. Fallback to heuristic SQL (still works)
```

**Q: Dashboard not refreshing**
```
Check: ANALYTICS_AUTO_REFRESH=true in .env
Check: ANALYTICS_REFRESH_INTERVAL_SEC value (in seconds)
Check: Exasol connected and table has data
```

**Q: Synthetic data not generating**
```
Check: SYNTHETIC_BATCHES value (must be > 0)
Check: mapping_output.json exists
Check: /synthetic_data/ directory writable
```

---

## 📚 Documentation

- **Pipeline Architecture**: [agent/PIPELINE_ARCHITECTURE.md](agent/PIPELINE_ARCHITECTURE.md) (if present)
- **Dashboard Code**: [dashboard/streamlit_app.py](dashboard/streamlit_app.py)
- **ETL Agents**: [agent/](agent/) folder contains all pipeline scripts

---

## 🔑 Key Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | Exasol (in-memory) | Fast analytics queries |
| **ETL** | Python (pandas, pyexasol) | Data transformation |
| **Dashboard** | Streamlit | Interactive UI |
| **AI/LLM** | Ollama (cloud) | Natural language SQL generation |
| **Data Formats** | Excel, JSON Lines | Input/output flexibility |

---

## 👥 Team Handoff Checklist

Before handing off to the next team:

- [ ] Clone repo and test on fresh machine
- [ ] Run `run_pipeline_files.sh` with sample data
- [ ] Run `run_pipeline_synthetic.sh` to test synthetic mode
- [ ] Open dashboard and test "Ask Data" feature
- [ ] Document any custom environment settings
- [ ] Update `.env.example` with new parameters
- [ ] Review & update this README with local learnings
- [ ] Verify all dependencies are in `requirements.txt`

---

---

## ❓ Troubleshooting Guide

### **Dashboard Issues**

#### **Problem: Dashboard won't start**
```bash
Error: "ModuleNotFoundError: No module named 'streamlit'"
```
**Solution:**
```bash
# Verify Python environment is activated
which python  # Should show venv path

# Reinstall dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Try again
streamlit run dashboard/streamlit_app.py
```

---

#### **Problem: "Ask Data" dialog not opening**
```
Click Ask button → nothing happens
```
**Solutions:**

1. **Check form submission** (Enter key)
   - Type question + Press Enter (should open dialog)
   - If not working, click "🔍 Ask" button instead

2. **Check session state**
   - Clear browser cache: Ctrl+Shift+Delete
   - Restart Streamlit: Kill terminal, run again

3. **Check Ollama API**
   ```bash
   curl -H "Authorization: Bearer $OLLAMA_API_KEY" \
        https://api.ollama.com/api/chat
   ```
   If fails → Ask Data uses heuristic SQL fallback (still works)

---

#### **Problem: Charts not rendering**
```
Error: "ValueError: x and y must be same length"
```
**Solution:**
- Usually means query returned no results
- Check data exists in Exasol:
  ```python
  import pyexasol
  conn = pyexasol.connect(host='127.0.0.1', port=8563, user='sys', password='exasol')
  result = conn.execute("SELECT COUNT(*) FROM OEE_UNIFIED")
  print(result.fetchall())
  ```

---

#### **Problem: Dashboard very slow/unresponsive**
**Checks:**
1. **Is refresh too frequent?**
   - Reduce `ANALYTICS_REFRESH_INTERVAL_SEC` in .env (e.g., 30s instead of 10s)
   - Or disable auto-refresh: `ANALYTICS_AUTO_REFRESH=false`

2. **Is Exasol under load?**
   - Check: `exasol-nano status`
   - Restart: `exasol-nano restart`

3. **Browser cache issue?**
   - Hard refresh: Ctrl+Shift+R (Windows/Linux) or Cmd+Shift+R (Mac)
   - Or clear cache: Settings → Privacy → Clear browsing data

---

### **Pipeline Issues**

#### **Problem: Pipeline script fails immediately**
```bash
./run_pipeline_files.sh
# Error: permission denied: ./run_pipeline_files.sh
```
**Solution:**
```bash
# Make scripts executable
cd agent/
chmod +x run_pipeline_files.sh run_pipeline_synthetic.sh

# Try again
./run_pipeline_files.sh
```

---

#### **Problem: Synthetic data generation hangs**
```bash
SYNTHETIC_BATCHES=5 ./run_pipeline_synthetic.sh
# Seems stuck...
```
**Solutions:**
1. **Get verbose output:**
   ```bash
   SYNTHETIC_BATCHES=2 ./run_pipeline_synthetic.sh 2>&1 | tee pipeline.log
   ```

2. **Reduce batch size:**
   ```bash
   SYNTHETIC_BATCHES=1 SYNTHETIC_ROWS_PER_FILE=10 ./run_pipeline_synthetic.sh
   ```

3. **Manually run pipeline step-by-step:**
   ```bash
   cd agent/
   python3 schema_detection_agent.py
   python3 mapping_agent.py
   python3 transform_load_agent.py  # This usually takes time
   ```

---

#### **Problem: "No data to load" error**
```
Error: OEE_UNIFIED table is empty
```
**Checklist:**
1. Did pipeline complete? Check for `✅ Load complete` message
2. Is Exasol connected? Check credentials in .env
3. Do data files exist?
   ```bash
   ls -la data/*.xlsx
   ls -la synthetic_data/*.jsonl
   ```

---

### **Database (Exasol) Issues**

#### **Problem: Cannot connect to Exasol**
```
Error: "Failed to connect to 127.0.0.1:8563"
```
**Troubleshooting:**
```bash
# Check if Exasol is running
exasol-nano status
# Should show: "Status: running"

# If not running, start it
exasol-nano start --memory-gb 2

# Wait 30-60 seconds for startup
sleep 60

# Test connection
nc -zv 127.0.0.1 8563
# Should say: "succeeded"

# Check Exasol logs
tail -f ~/.exanano/logs/exasol.log
```

---

#### **Problem: "Permission denied" in Exasol**
```
Error: "User 'sys' does not have permission"
```
**Solution:**
1. Check credentials in `.env`:
   ```bash
   EXASOL_USER=sys
   EXASOL_PASSWORD=exasol
   ```
   (Default credentials - change in production!)

2. Manual Exasol login to verify:
   ```bash
   # From Exasol install directory
   exasol-nano login --user sys --password exasol
   ```

---

### **Environment & Configuration Issues**

#### **Problem: ".env file not found"**
```bash
Error: FileNotFoundError: [Errno 2] No such file or directory: '.env'
```
**Solution:**
```bash
# Create .env from template
cp .env.example .env

# Edit with your settings
nano .env

# Minimally required:
EXASOL_HOST=127.0.0.1
EXASOL_PORT=8563
EXASOL_USER=sys
EXASOL_PASSWORD=exasol
OLLAMA_API_KEY=your-key-here
```

---

#### **Problem: "Invalid API key" for Ollama**
```
Error: HTTP 401 Unauthorized
```
**Solution:**
1. Verify API key:
   ```bash
   grep OLLAMA_API_KEY .env
   ```

2. Test API key directly:
   ```bash
   curl -X POST https://api.ollama.com/api/chat \
        -H "Authorization: Bearer YOUR_KEY" \
        -H "Content-Type: application/json" \
        -d '{"model":"qwen3-coder-next","messages":[{"role":"user","content":"SELECT * FROM table"}]}'
   ```

3. If fails consistently → Ask Data uses heuristic SQL (fallback works)

---

### **Data Quality Issues**

#### **Problem: Dashboard shows "No data" or all zeros**
```
All KPI cards showing 0 or "--"
```
**Checks:**
1. **Table exists?**
   ```python
   import pyexasol
   conn = pyexasol.connect(...)
   result = conn.execute("SHOW TABLES LIKE 'OEE%'")
   print(result.fetchall())
   ```

2. **Table has data?**
   ```python
   result = conn.execute("SELECT COUNT(*) FROM OEE_UNIFIED")
   print(result.fetchall())  # Should be > 0
   ```

3. **Columns correctly named?**
   ```python
   result = conn.execute("DESCRIBE OEE_UNIFIED")
   print(result.fetchall())
   ```

---

#### **Problem: Some plants missing from Top 5 chart**
```
Only 3 plants showing in "Top 5 Plants by OEE"
```
**Explanation:**
- Chart shows available data (some plants may have no data)
- Load more data:
  ```bash
  cd agent/
  SYNTHETIC_BATCHES=5 ./run_pipeline_synthetic.sh
  ```

---

### **Performance Issues**

#### **Problem: Queries are slow (<30 sec)**
```
Dashboard takes >15 seconds to load after click
```
**Optimization:**
1. **Reduce refresh interval:**
   ```env
   ANALYTICS_REFRESH_INTERVAL_SEC=30  # From 10
   ANALYTICS_CACHE_TTL_SEC=600        # From 300
   ```

2. **Clear cache:**
   ```bash
   # Restart Streamlit will clear all caches
   # Ctrl+C then: streamlit run dashboard/streamlit_app.py
   ```

3. **Check table size:**
   ```python
   # If table > 1 million rows, performance degrades
   # Archive old data or add indexes
   ```

---

### **Getting Help**

**Check logs first:**
```bash
# Pipeline logs
tail -20 /tmp/pipeline.log

# Exasol logs
tail -50 ~/.exanano/logs/exasol.log

# Streamlit logs (in terminal where you ran streamlit)
# Just scroll up in the same terminal
```

**Debug mode (verbose output):**
```bash
# If .env has DEBUG_MODE=true
python3 transform_load_agent.py --debug
streamlit run dashboard/streamlit_app.py --logger.level=debug
```

---

## 📞 Support Contact

- **Database Issues**: Check Exasol logs (`~/.exanano/logs/`)
- **Pipeline Issues**: Check agent logs (stdout of pipeline scripts)
- **Dashboard Issues**: Check browser console (F12) + Streamlit logs
- **Documentation**: See docs/ folder for detailed architecture

---

## 📄 License

Internal project - All rights reserved

---

**Last Updated:** March 27, 2026  
**Status:** Production-Ready ✅

### Phase 1: Near Real-Time Analytics (Auto-Refresh)

The dashboard now includes **automatic data change detection** and **intelligent refresh**:

**Features:**
- ✅ **Auto-Detection**: Monitors table for row count & timestamp changes every N seconds
- ✅ **Automatic Refresh**: Triggers dashboard update only when data changes (no manual trigger needed)
- ✅ **Configurable Intervals**: Adjust check frequency via Runtime Controls (5-60 seconds)
- ✅ **Smart Caching**: Query results cached for configurable TTL (default 300s)
- ✅ **Thread-Safe Async Workers**: Background query execution prevents UI blocking

**Configuration** (in `.env`):
```bash
# Check for changes every 10 seconds
ANALYTICS_REFRESH_INTERVAL_SEC=10

# Cache query results for 5 minutes
ANALYTICS_CACHE_TTL_SEC=300

# Enable auto-refresh (true/false)
ANALYTICS_AUTO_REFRESH=true
```

**How It Works:**
1. On each dashboard rerun, checks if table row count or max timestamp changed
2. If change detected → invalidates cache → triggers full dashboard refresh
3. Users see fresh data automatically without manual intervention
4. Refresh interval is user-configurable via UI slider in Runtime Controls

**Modules:**
- `change_detector.py` - Monitors table for modifications
- `async_query_worker.py` - Background query execution with caching

**UI Improvements:**
- "Quick Ask" panel redesigned for better usability (top-right widget)
- Runtime Controls now show Auto-Refresh settings
- Manual refresh button available as backup
- Last updated timestamp visible on dashboard

## CLI (Sample Working Conditions)

Initialize connector tables:

```bash
python scripts/mes_connector_cli.py init
```

Create schema dynamically:

```bash
python scripts/mes_connector_cli.py schema-create --name TEAM_SCHEMA
```

Create table dynamically with explicit SQL types:

```bash
python scripts/mes_connector_cli.py \
  table-create \
  --table TEAM_OUTPUT \
  --target-schema TEAM_SCHEMA \
  --columns 'id:VARCHAR(64),plant_name:VARCHAR(100),score:DECIMAL(18,6),event_date:DATE'
```

Ensure table dynamically from incoming JSON keys (auto-add missing columns):

```bash
python scripts/mes_connector_cli.py \
  table-ensure \
  --table TEAM_OUTPUT \
  --target-schema TEAM_SCHEMA \
  --file scripts/sample_raw_rows.json
```

Insert normalized rows from JSON:

```bash
python scripts/mes_connector_cli.py normalized-insert --file scripts/sample_normalized.json
```

Query normalized rows:

```bash
python scripts/mes_connector_cli.py normalized-query --limit 50
```

Remove normalized rows:

```bash
python scripts/mes_connector_cli.py normalized-remove --where "\"plant_name\" = 'Plant 01'"
```

Insert raw rows from JSON:

```bash
python scripts/mes_connector_cli.py raw-insert --file scripts/sample_raw_rows.json
```

Raw row keys are stored as real columns (not dumped into one JSON payload column). New keys are added automatically to the raw table.

Query selected columns from any table:

```bash
python scripts/mes_connector_cli.py table-query \
  --table NORMALIZED_UNIFIED \
  --columns plant_name,date,oee_normalized \
  --where '"plant_name" IN ('\''Plant 01'\'','\''Plant 2'\'')' \
  --limit 100
```

Upload raw files (`.json`, `.xlsx`, `.xlsm`, `.xls`) from `data/`:

```bash
python scripts/mes_connector_cli.py file-upload-dir --path data --remote-prefix plants
```

List stored files:

```bash
python scripts/mes_connector_cli.py file-list --prefix plants --limit 100
```

Download a stored file:

```bash
python scripts/mes_connector_cli.py \
  file-download --remote-path plants/plant_P01.xlsx --out /tmp/plant_P01.xlsx
```

Delete a stored file:

```bash
python scripts/mes_connector_cli.py file-remove --remote-path plants/plant_P01.xlsx
```

---

## Direct Python Usage (Advanced)

For custom integrations, use `pyexasol` directly:

```python
import pyexasol

# Connect to Exasol
conn = pyexasol.connect(
    host='127.0.0.1',
    port=8563,
    user='sys',
    password='exasol',
    schema='HACKATHON'
)

# Query data
result = conn.execute(
    'SELECT * FROM OEE_UNIFIED WHERE plant_id = :plant_id LIMIT 10',
    {'plant_id': 'P01'}
)
print(result.fetchall())

# Insert normalized data
conn.execute('''
    INSERT INTO OEE_UNIFIED 
    (plant_id, date, availability, performance, quality, oee_normalized) 
    VALUES (?, ?, ?, ?, ?, ?)
''', ('P99', '2026-03-27', 0.92, 0.88, 0.98, 0.79))

conn.close()
```

**Note:** For standard data loading workflows, use the pipeline scripts instead:
- `run_pipeline_files.sh` - Load Excel files
- `run_pipeline_synthetic.sh` - Generate synthetic data
- `transform_load_agent.py` - Manual transformation & load
