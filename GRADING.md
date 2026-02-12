# Grading Guide: Data Engineering 2 - Modern Data Platforms

This document instructs an AI grading agent (Claude Code) how to grade student submissions for the Databricks/Spark homework assignment.

## Input Format

Submissions arrive as `.ipynb` files exported from Databricks. Each notebook contains:
- **Markdown cells**: Text/documentation cells. In Databricks exports, these typically start with `%md` or have `"cell_type": "markdown"`.
- **Code cells**: Python/SQL cells. Look at both `source` content and `outputs` (if present). Databricks magic commands like `%sql`, `%fs`, `%md` appear as the first line of a code cell's source.

## How to Grade

Read the entire notebook. For each section below, check whether the requirement is met and assign points. Be **generous on intent, strict on presence**:
- If a student clearly attempted a requirement and the code is reasonable, give full points even if the output is missing (outputs may not survive export).
- If the code is present but has a minor syntax issue, give most of the points.
- If the requirement is completely absent, give 0 for that sub-item.
- Do NOT deduct points for style preferences, variable naming, or approach choices -- only for missing requirements.

When a requirement says "meaningful", it means the operation should relate to the dataset in a way that makes analytical sense (e.g., filtering for a specific country, not filtering where 1==1).

## Grading Rubric

### 1. Data Ingestion & Exploration (15 points)

**1a. Explore with dbutils (3 pts)**
- Look for: `dbutils.fs.ls(...)` call or a `%fs ls ...` magic command targeting the dataset path.
- **3 pts**: Present and targets the correct dataset folder.
- **0 pts**: Absent.

**1b. Load with schema inference (4 pts)**
- Look for: `spark.read.csv(...)` with `header=True` (or `header="true"`) and `inferSchema=True` (or `inferSchema="true"`).
- Also look for: `display(df)` or `df.show()` AND `df.printSchema()` (or `.schema` or `.dtypes`).
- **4 pts**: CSV loaded with inferSchema, DataFrame displayed, and schema printed.
- **3 pts**: CSV loaded with inferSchema but missing either display or schema print.
- **2 pts**: CSV loaded but inferSchema not explicitly set (or set to False).
- **0 pts**: No CSV loading found.

**1c. Load with manual schema (4 pts)**
- Look for: A DDL-formatted string schema (e.g., `"col1 INT, col2 STRING, ..."`) passed to `spark.read.schema(...)` or as the `schema` parameter. Also accept `StructType`/`StructField` definitions.
- **4 pts**: Manual schema defined and used to load data. At least 5 columns defined (or all columns if fewer than 5).
- **3 pts**: Manual schema present but fewer than 5 columns defined (and the dataset has more than 5).
- **2 pts**: Schema is defined but not actually used in the read call.
- **0 pts**: No manual schema found.

**1d. Row count and sample (4 pts)**
- Look for: `.count()` called and result displayed/printed. Also `display(df)` or `df.show()` for sample rows.
- **4 pts**: Both count and sample display present.
- **2 pts**: Only one of count or sample display present.
- **0 pts**: Neither present.

---

### 2. SQL Queries (10 points)

**2a. Create a temp view (3 pts)**
- Look for: `df.createOrReplaceTempView("...")` (or `createTempView` or `createGlobalTempView`).
- Also look for: `spark.table("...")` used to read it back into a variable.
- **3 pts**: Both createOrReplaceTempView and spark.table() present.
- **2 pts**: createOrReplaceTempView present but spark.table() missing (or vice versa).
- **0 pts**: Neither present.

**2b. SQL queries (7 pts)**
- Look for: At least 2 SQL queries via `%sql` magic cells or `spark.sql("...")` calls.
- Each query must answer a meaningful question (not just `SELECT *`).
- The 2 queries must be different in nature (e.g., one filtering, one aggregating; not two nearly identical queries on different columns).
- **7 pts**: 2+ distinct, meaningful SQL queries present.
- **5 pts**: 2 queries present but very similar in nature (both just SELECTs with WHERE, etc.).
- **3 pts**: Only 1 meaningful SQL query.
- **0 pts**: No SQL queries found.

---

### 3. DataFrame Transformations (20 points)

**3a. filter/where (5 pts)**
- Look for: `.filter(...)` or `.where(...)` calls with meaningful conditions.
- **5 pts**: 2+ different filter conditions applied meaningfully.
- **3 pts**: Only 1 filter condition, or 2 filters that are trivially similar.
- **0 pts**: No filter/where usage found.

**3b. select + withColumn (5 pts)**
- Look for: `.select(...)` choosing specific columns (not `select("*")`).
- Look for: `.withColumn("new_col", ...)` creating a derived/computed column.
- **5 pts**: Both select (with specific columns) and withColumn (with a computed expression) present.
- **3 pts**: Only one of select or withColumn present, or withColumn just renames/casts without computation.
- **0 pts**: Neither present.

**3c. sort/orderBy + limit (5 pts)**
- Look for: `.sort(...)` or `.orderBy(...)` AND `.limit(...)` used together (or in close proximity).
- **5 pts**: Both sort and limit used meaningfully.
- **3 pts**: Only sort or only limit present.
- **0 pts**: Neither present.

**3d. distinct or dropDuplicates (5 pts)**
- Look for: `.distinct()` or `.dropDuplicates(...)` call.
- Look for: A markdown cell nearby explaining what duplicates exist or don't exist.
- **5 pts**: distinct/dropDuplicates used AND explanation in markdown.
- **3 pts**: distinct/dropDuplicates used but no explanation, OR explanation present but no code.
- **0 pts**: Neither present.

---

### 4. Aggregations (15 points)

**4a. groupBy with aggregation methods (5 pts)**
- Look for: `df.groupBy("...").avg()`, `.count()`, `.sum()`, `.max()`, `.min()`, `.mean()` -- at least 2 different aggregation methods.
- These are the shorthand methods called directly on the GroupedData object (not inside `.agg()`).
- **5 pts**: groupBy used with 2+ different built-in aggregation methods.
- **3 pts**: groupBy used with only 1 aggregation method.
- **0 pts**: No groupBy with shorthand methods found.

**4b. groupBy with .agg() -- SELF-LEARNING (10 pts)**
- Look for: `.groupBy("...").agg(...)` with named functions from `pyspark.sql.functions` inside `.agg()`.
- Count the distinct aggregate functions used inside `.agg()`. Examples: `avg("col")`, `sum("col")`, `countDistinct("col")`, `stddev("col")`, `collect_list("col")`, `var_pop("col")`, `min("col")`, `max("col")`, `count("col")`. Students may import these as `from pyspark.sql.functions import avg, sum` or as `from pyspark.sql import functions as F` and use `F.avg()` -- both patterns are valid.
- At least one must be a function NOT directly demonstrated in the class walkthrough. Functions demonstrated in class: `avg`, `sum`, `min`, `max`, `count`, `mean`, `approx_count_distinct`. Self-learned examples: `stddev`, `stddev_samp`, `stddev_pop`, `var_pop`, `var_samp`, `collect_list`, `collect_set`, `countDistinct`, `corr`, `covar_pop`, `covar_samp`, `first`, `last`, `kurtosis`, `skewness`, `percentile_approx`, `sumDistinct`.
- **Note**: `avg` and `mean` are aliases in PySpark and count as 1 distinct function, not 2.
- **10 pts**: `.agg()` used with 3+ distinct aggregate functions AND at least 1 is self-learned (not from the demonstrated list).
- **7 pts**: `.agg()` used with 3+ functions but none are self-learned, OR `.agg()` with 2 functions where 1 is self-learned.
- **4 pts**: `.agg()` used with only 1-2 functions total (regardless of whether they are demonstrated or self-learned).
- **2 pts**: `.agg()` called but with trivial or incorrect usage.
- **0 pts**: No `.agg()` usage found.

---

### 5. Visualizations (8 points)

- Look for: `display(df)` calls that produce chart outputs. In Databricks exports, chart configuration may appear in cell metadata or outputs. Also look for comments/markdown mentioning chart types, or `displayHTML` with chart libraries.
- The 2 charts must be **different types** (bar, line, pie, scatter, area, histogram, etc.).
- **8 pts**: 2+ visualizations of different chart types found (or clearly configured in display calls with chart metadata).
- **6 pts**: 2 visualizations but same chart type, or chart type unclear from export.
- **3 pts**: Only 1 visualization found.
- **0 pts**: No visualizations found.

**Note on exports**: Databricks `.ipynb` exports may not always include the chart rendering. If you see `display(df)` calls with markdown cells above them saying things like "bar chart of X" or "scatter plot of Y", give credit. The student likely configured the chart in the UI and it didn't survive the export. Be generous here.

---

### 6. Delta Lake (10 points)

**6a. Write to Delta (3 pts)**
- Look for: `.write.format("delta").save(...)` or `.write.save(..., format="delta")` or `.write.mode("overwrite").format("delta").save(...)`. The path should reference `DA.paths.workdir` or the target volume path.
- **3 pts**: Delta write present targeting the correct location.
- **2 pts**: Delta write present but targeting a different/unclear location.
- **0 pts**: No Delta write found.

**6b. Verify Delta structure (3 pts)**
- Look for: `dbutils.fs.ls(...)` or `%fs ls` on the Delta output folder, showing `_delta_log/` in results.
- Look for: A markdown cell explaining what `_delta_log` is.
- **3 pts**: Both the ls verification AND the markdown explanation present.
- **2 pts**: Only the ls call, or only the explanation.
- **0 pts**: Neither present.

**6c. saveAsTable + SQL query (4 pts)**
- Look for: `.write.saveAsTable("...")` or `.saveAsTable("...")` call.
- Look for: A subsequent `%sql SELECT ... FROM table_name` or `spark.sql("SELECT ... FROM table_name")` querying the registered table.
- **4 pts**: Both saveAsTable and a SQL query against the table present.
- **2 pts**: Only saveAsTable present, or only the SQL query on a table that wasn't created with saveAsTable.
- **0 pts**: Neither present.

---

### 7. Additional pyspark.sql.functions -- SELF-LEARNING (5 points)

- Look for: Usage of `pyspark.sql.functions` beyond what is already required by other sections.
- **No-double-counting rule**: Functions the student already used inside `.agg()` in Section 4b should not be counted again here. For example, if a student used `F.stddev()` inside `.agg()`, it cannot also count for Section 7. However, functions used in other contexts (e.g., `when()` inside a `withColumn` in Section 3) CAN also be counted for Section 7, because the no-double-counting rule applies specifically to aggregate functions in `.agg()`.
- Valid additional functions include: `when`/`otherwise`, `round`, `ceil`, `floor`, `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `concat`, `substring`, `split`, `regexp_extract`, `regexp_replace`, `to_date`, `to_timestamp`, `date_format`, `year`, `month`, `dayofweek`, `datediff`, `date_add`, `abs`, `sqrt`, `log`, `exp`, `pow`, `coalesce`, `isnull`, `isnan`, `array`, `explode`, `size`, `element_at`, `struct`, `translate`, `length`, `initcap`, `lpad`, `rpad`, `format_number`, `percent_rank`, `dense_rank`, `row_number`, `lag`, `lead`, etc.
- Do NOT count `col()` or `lit()` as they are utility functions, not transformations.
- **5 pts**: 2+ additional functions used meaningfully.
- **3 pts**: Only 1 additional function used.
- **0 pts**: No additional functions beyond what's required.

---

### 8. Null Handling with DataFrameNaFunctions -- SELF-LEARNING (5 points)

- Look for: `.na.drop(...)` or `.na.fill(...)` or `df.dropna(...)` or `df.fillna(...)`.
- Look for: A markdown cell explaining the reasoning (why drop/fill, what values used).
- **5 pts**: na.drop or na.fill used meaningfully AND reasoning explained in markdown.
- **3 pts**: na.drop or na.fill used but no explanation, OR explanation but trivial usage (e.g., `na.fill(0)` on all columns without thought).
- **0 pts**: No null handling found.

---

### 9. Notebook Presentation (5 points)

- Scan the entire notebook structure.
- Count code cells and markdown cells.
- Check: Does the notebook have at least 20 code cells?
- Check: Do most code cells (80%+) have a markdown cell above them?
- Check: Are there markdown headings (`#`, `##`, `###`) organizing the notebook into sections?
- Check: Are the markdown explanations meaningful (not just "code below" but actually explaining what and why)?
- **5 pts**: 20+ code cells, 80%+ have markdown above, headings organize sections, explanations are meaningful.
- **4 pts**: 20+ code cells, most have markdown above, but some explanations are perfunctory or headings are sparse.
- **3 pts**: Fewer than 20 code cells OR significant gaps in markdown documentation (50-80% coverage).
- **2 pts**: Markdown cells present but minimal and generic (<50% of code cells have explanations), though some headings exist.
- **1 pt**: Markdown cells very sparse -- only a few scattered explanations with no organizational structure.
- **0 pts**: No meaningful markdown documentation.

---

### 10. Data Story (7 points)

- Look for: Dedicated markdown cells (not just code-explanation cells) containing analytical paragraphs about findings.
- The writing should interpret results, not describe code. For example: "We found that the average temperature has increased by 1.2 degrees since 1900, with acceleration after 1980" is analytical. "The groupBy above groups by year and computes the average" is just code description.
- Must reference at least 2 different query/aggregation results from the notebook.
- **7 pts**: 2-3 paragraphs of genuine analytical insight, referencing 2+ results, with observations about patterns, surprises, or next steps.
- **5 pts**: Some analytical writing present but shallow (mostly restating numbers without interpretation) or references only 1 result.
- **3 pts**: A brief attempt at analysis (1 short paragraph or a few bullet points) but lacking depth.
- **1 pt**: A single sentence of analysis buried in a code-explanation cell.
- **0 pts**: No analytical writing found -- only code descriptions.

---

## Output Format

After grading, produce a structured report in the following format:

```
# Grading Report: [Student/Group Name or Filename]

## Scores

| # | Section | Max | Score | Notes |
|---|---------|-----|-------|-------|
| 1 | Data Ingestion & Exploration | 15 | __ | |
| 1a | - Explore with dbutils | 3 | __ | |
| 1b | - Load with schema inference | 4 | __ | |
| 1c | - Load with manual schema | 4 | __ | |
| 1d | - Row count and sample | 4 | __ | |
| 2 | SQL Queries | 10 | __ | |
| 2a | - Create a temp view | 3 | __ | |
| 2b | - SQL queries | 7 | __ | |
| 3 | DataFrame Transformations | 20 | __ | |
| 3a | - filter/where | 5 | __ | |
| 3b | - select + withColumn | 5 | __ | |
| 3c | - sort/orderBy + limit | 5 | __ | |
| 3d | - distinct/dropDuplicates | 5 | __ | |
| 4 | Aggregations | 15 | __ | |
| 4a | - groupBy with methods | 5 | __ | |
| 4b | - groupBy with .agg() | 10 | __ | |
| 5 | Visualizations | 8 | __ | |
| 6 | Delta Lake | 10 | __ | |
| 6a | - Write to Delta | 3 | __ | |
| 6b | - Verify Delta structure | 3 | __ | |
| 6c | - saveAsTable + SQL | 4 | __ | |
| 7 | Additional pyspark.sql.functions | 5 | __ | |
| 8 | Null Handling | 5 | __ | |
| 9 | Notebook Presentation | 5 | __ | |
| 10 | Data Story | 7 | __ | |
| | **TOTAL** | **100** | **__** | |

## Detailed Feedback

[For each section, write 1-2 sentences explaining what was found and why the score was given. Cite specific cell numbers or code snippets where relevant.]

## Summary

[2-3 sentences overall assessment: strengths, areas for improvement.]
```

## Important Grading Principles

1. **Be generous on format, strict on substance.** Databricks exports can lose chart renders, output cells, and formatting. If the code clearly does the right thing, give credit even if outputs are missing.

2. **Do not double-count.** If a student uses `F.countDistinct()` inside `.agg()` for section 4b, do NOT also count it for section 7 (additional functions). Prefer counting it toward the section where it's explicitly required. See Section 7's specific no-double-counting rule for details on what counts.

3. **Accept reasonable alternatives.** For example:
   - `df.schema` instead of `df.printSchema()` is fine for showing the schema.
   - `df.show()` instead of `display(df)` is fine for displaying data.
   - `spark.sql("CREATE OR REPLACE TEMP VIEW ...")` instead of `df.createOrReplaceTempView()` is acceptable.
   - `.write.mode("overwrite").saveAsTable(...)` counts for saveAsTable.
   - SQL queries written in `%sql` cells count the same as `spark.sql()` calls.

4. **"Meaningful" means related to the data.** A filter like `.filter(col("price") > 100)` on a product dataset is meaningful. A filter like `.filter(lit(True))` is not.

5. **Self-learning sections deserve extra credit for effort.** If a student attempts a self-learning task and gets it mostly right but with a minor issue, lean toward giving most of the points. The point of these sections is to reward initiative.

6. **Code cell count**: Count cells that contain executable code (Python or SQL). Do not count markdown-only cells. `%sql`, `%fs`, and `%md` magic cells: count `%sql` and `%fs` as code cells, do NOT count `%md` as code cells.

7. **Classroom-Setup cell**: Do not deduct points for the presence or absence of `%run ./Includes/Classroom-Setup`. This is infrastructure setup, not a graded requirement.

8. **Duplicate datasets**: The "Used Cars" and "Vehicle Sales Data" datasets in the assignment may contain the same underlying data (`car_prices.csv`, 84 MB). Treat submissions using either dataset identically -- do not penalize for the choice.
