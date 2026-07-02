# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow"]
# ///
#
# Usage: uv run fixtures/gen_sample.py
# Generates fixtures/sample_10k.parquet (not committed).

import random
import string
import os

import pyarrow as pa
import pyarrow.parquet as pq

random.seed(42)
N = 10_000
OUT = os.path.join(os.path.dirname(__file__), "sample_10k.parquet")

table = pa.table({
    "id": pa.array(range(N), type=pa.int64()),
    "name": pa.array(
        ["".join(random.choices(string.ascii_lowercase, k=random.randint(3, 20))) for _ in range(N)],
        type=pa.utf8(),
    ),
    "price": pa.array(
        [round(random.uniform(0.5, 999.99), 2) for _ in range(N)],
        type=pa.float64(),
    ),
    "quantity": pa.array(
        [random.randint(1, 1000) for _ in range(N)],
        type=pa.int32(),
    ),
    "active": pa.array(
        [random.choice([True, False]) for _ in range(N)],
        type=pa.bool_(),
    ),
    "category": pa.array(
        [random.choice(["electronics", "clothing", "food", "books", "toys"]) for _ in range(N)],
        type=pa.utf8(),
    ),
    "description": pa.array(
        ["".join(random.choices(string.ascii_letters + " ", k=random.randint(10, 200))) for _ in range(N)],
        type=pa.utf8(),
    ),
    "latitude": pa.array(
        [round(random.uniform(25.0, 45.0), 6) for _ in range(N)],
        type=pa.float64(),
    ),
    "longitude": pa.array(
        [round(random.uniform(125.0, 145.0), 6) for _ in range(N)],
        type=pa.float64(),
    ),
})

pq.write_table(table, OUT, row_group_size=2000)

size = os.path.getsize(OUT)
print(f"{OUT}: {N} rows, {table.num_columns} cols, {N // 2000} row groups, {size / 1024:.0f} KB")
