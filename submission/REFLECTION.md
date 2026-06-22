# Reflection: Top Lakehouse Anti-Patterns

Our team's data operations are most at risk of the **"Small Files" Death Spiral** anti-pattern. 

In our high-throughput LLM observability pipeline logging over 200K requests (and scaling up to 1B requests/day in production), data arrives continuously in near real-time. If we write these incoming streams directly into the Bronze layer in micro-batches, it creates thousands of tiny Parquet files daily. 

Without routine maintenance, query engines (like DuckDB, Trino, or Spark) experience severe performance degradation due to massive metadata overhead during file-open and query-planning stages. In our experiments (NB2), querying user events across 200 uncompacted files was highly inefficient.

By applying Delta Lake's native **compaction** (`OPTIMIZE`) and multi-dimensional clustering (**Z-ordering** on `user_id` or `model`), we co-locate related records and consolidate small files into larger, standardized shards. This allows the engine to prune irrelevant files based on min/max statistics in the transaction log, decreasing query latencies from seconds to milliseconds and achieving a deterministic files-pruned speedup ratio of over 10×.
