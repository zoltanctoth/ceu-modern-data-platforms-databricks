# CEU Modern Data Platforms - Databricks

Courseware for Databricks/Spark module.

## Sync to Databricks

Bidirectional sync with Databricks workspace:

```bash
.dev/sync.sh
```

This uses OAuth authentication with profile `ceu-prep-2026` and syncs bidirectionally with `/Workspace/Users/zoltan+de2prep2026@nordquant.com/ceu-modern-data-platforms-databricks`.

### Re-authenticate if needed

```bash
/usr/local/bin/databricks auth login --host https://dbc-b134e182-2f19.cloud.databricks.com --profile ceu-prep-2026
```

## Project Structure

- `00 - Databricks Introduction/` - Platform basics, dataset exploration
- `01 - Spark Core/` - Spark SQL, DataFrames, readers/writers
- `02 - Aggregations and Functions/` - Aggregations, datetimes, complex types, UDFs
- `03 - Tasks, Jobs and Stages.py` - Spark execution model
- `Reference 1 - Performance/` - Query optimization, partitioning
- `Reference 2 - Delta Lake/` - Delta Lake features
- `Includes/` - Shared setup scripts
- `.dev/` - Development scripts (not courseware)
