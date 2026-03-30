# 🏭 OEE Pipeline Architecture - Refactored

## Overview

The pipeline has been completely refactored for clarity and modularity:

- **Synthetic Data**: Generated as **streaming JSON (JSONL)** files
- **File Data**: Read from Excel/JSON files in `/data/`
- **Transformation & Loading**: Uses **identical logic** for both sources
- **Pipeline Scripts**: **Separate scripts** for synthetic vs file modes

---

## 📂 File Structure

```
agent/
├── run_pipeline.sh                 # Original (backward compatible)
├── run_pipeline_synthetic.sh       # NEW - Synthetic mode pipeline
├── run_pipeline_files.sh           # NEW - File mode pipeline
├── synthetic_data_generator.py     # NEW - Generates .jsonl files
├── transform_load_agent.py         # UPDATED - Reads .jsonl & Excel
├── schema_detection_agent.py       # (unchanged)
├── mapping_agent.py                # (unchanged)
└── query_agent.py                  # (unchanged)

data/
└── *.xlsx                          # Input Excel files

synthetic_data/
└── *.jsonl                         # Generated synthetic data (JSON Lines)
```

---

## 🚀 Usage

### **Option 1: Synthetic Data Pipeline**

```bash
cd agent/
./run_pipeline_synthetic.sh
```

**Configuration** (via environment variables):

```bash
SYNTHETIC_BATCHES=4 \
SYNTHETIC_ROWS_PER_FILE=8 \
SYNTHETIC_INTERVAL_SEC=3 \
SYNTHETIC_SEED=42 \
./run_pipeline_synthetic.sh
```

**What happens:**
1. ✅ Starts Exasol
2. ✅ Runs Schema Detection
3. ✅ Runs Mapping Agent
4. ✅ **Generates** synthetic data → `/synthetic_data/*.jsonl`
5. ✅ **Transforms** JSONL data
6. ✅ **Loads** into Exasol
7. ✅ Runs Query Agent

---

### **Option 2: File Mode Pipeline**

```bash
cd agent/
./run_pipeline_files.sh
```

**What happens:**
1. ✅ Starts Exasol
2. ✅ Runs Schema Detection
3. ✅ Runs Mapping Agent
4. ✅ **Reads** Excel files from `/data/`
5. ✅ **Transforms** data
6. ✅ **Loads** into Exasol
7. ✅ Runs Query Agent

---

## 📊 Synthetic Data Format (JSONL)

Generated files are in **JSON Lines** format (one JSON object per line), perfect for streaming:

**Example: `plant_P01.xlsx.jsonl`**
```json
{"plant_id":"P01","date":"2026-03-25","planned_hours":11.234,"run_hours":8.456,"units_produced":4523,"units_defective":186}
{"plant_id":"P01","date":"2026-03-26","planned_hours":9.876,"run_hours":7.892,"units_produced":3891,"units_defective":142}
```

**Supported Schemas:**

1. **production**
   - Fields: `plant_id`, `date`, `planned_hours`, `run_hours`, `units_produced`, `units_defective`
   - Sample output shown above

2. **kpi**
   - Fields: `plant_id`, `date`, `availability_pct`, `performance_pct`, `quality_pct`
   ```json
   {"plant_id":"P01","date":"2026-03-25","availability_pct":78.45,"performance_pct":89.23,"quality_pct":92.10}
   ```

3. **event**
   - Fields: `plant_id`, `date`, `event_type`, `machine`, `duration_sec`
   ```json
   {"plant_id":"P01","date":"2026-03-25","event_type":"RUN","machine":"M-05","duration_sec":1854}
   ```

4. **time**
   - Fields: `plant_id`, `date`, `total_hours`, `operating_hours`, `oee`
   ```json
   {"plant_id":"P01","date":"2026-03-25","total_hours":10.234,"operating_hours":8.912,"oee":0.6723}
   ```

5. **quality**
   - Fields: `plant_id`, `date`, `total_units`, `defective_units`, `downtime`, `reason`
   ```json
   {"plant_id":"P01","date":"2026-03-25","total_units":2456,"defective_units":89,"downtime":23.5,"reason":"maintenance"}
   ```

---

## 🔍 Visibility & Verification

The synthetic data generator logs **sample records** for each schema to verify correctness:

```
📦 Batch 1/4
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
✅ plant_P01.xlsx                  (production ) → 8 records
   📋 Sample: {"plant_id":"P01","date":"2026-03-25","planned_hours":11.234,"run_hours":8.456,"units_produced":4523,"units_defective":186}
✅ plant_P02.xlsx                  (kpi        ) → 8 records
   📋 Sample: {"plant_id":"P02","date":"2026-03-25","availability_pct":78.45,"performance_pct":89.23,"quality_pct":92.10}
✅ plant_P03.xlsx                  (event      ) → 8 records
   📋 Sample: {"plant_id":"P03","date":"2026-03-25","event_type":"RUN","machine":"M-05","duration_sec":1854}
```

---

## Configuration Parameters

**Synthetic Generation** (environment variables):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SYNTHETIC_BATCHES` | 4 | Number of batches (0 = infinite) |
| `SYNTHETIC_ROWS_PER_FILE` | 8 | Rows per file per batch |
| `SYNTHETIC_INTERVAL_SEC` | 3 | Seconds between batches |
| `SYNTHETIC_SEED` | (empty) | Random seed for reproducibility |

**Example:**
```bash
# Generate 10 batches with 16 rows each, repeatable data
SYNTHETIC_BATCHES=10 \
SYNTHETIC_ROWS_PER_FILE=16 \
SYNTHETIC_SEED=12345 \
./run_pipeline_synthetic.sh
```

---

## Transformation Logic (Both Modes)

The same `transform_load_agent.py` processes both modes:

1. **Read** from either `/data/*.xlsx` or `/synthetic_data/*.jsonl`
2. **Transform** based on schema (production, kpi, event, time, quality)
3. **Normalize** to unified columns (plant_id, date, planned_time_min, etc.)
4. **Calculate** OEE: `oee = availability × performance × quality`
5. **Load** into Exasol UNIFIED_KPI table

---

## Key Advantages

✅ **Clean Separation**: Data generation ≠ transformation/loading  
✅ **Streaming Format**: JSONL is perfect for real-time pipelines  
✅ **Verification**: Sample records logged for each schema type  
✅ **Identical Logic**: Both modes use exact same transformation  
✅ **Modularity**: Each script has single responsibility  
✅ **Flexibility**: Run generator separately if needed  

---

## Running Individual Components

```bash
# Generate synthetic data only (no Exasol needed)
cd agent/
SYNTHETIC_BATCHES=2 python3 synthetic_data_generator.py

# Transform & load only (data already in place)
cd agent/
PIPELINE_DATA_MODE=synthetic python3 transform_load_agent.py

# Or for Excel files
PIPELINE_DATA_MODE=files python3 transform_load_agent.py
```

---

## Troubleshooting

**Synthetic data not being generated?**
- Check `synthetic_data_generator.py` logs
- Verify `SYNTHETIC_BATCHES` > 0
- Check `/synthetic_data/` for `.jsonl` files

**Files not being read?**
- Ensure Excel files are in `/data/`
- Check mapping_output.json has correct file names
- Verify schema names match MES_TYPES

**Generated data doesn't match schema?**
- Run generator with logging: `python3 synthetic_data_generator.py`
- Check sample JSON output for each file
- Verify schema mapping is correct
