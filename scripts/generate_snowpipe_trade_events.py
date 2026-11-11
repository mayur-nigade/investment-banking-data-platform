#!/usr/bin/env python3
"""Create a six-event synthetic trade Parquet to test Snowpipe -> Iceberg -> Gold.

Python: 3.9+
Install: python -m pip install pyarrow
Run: python scripts/generate_snowpipe_trade_events.py
Target: s3://omera-ib-trade-lake/landingzone/parquet/auto/

The client/account/instrument combinations below match prior fictional records
from the starter ZIP. No private bank/client data. Fixed EVENT_IDs -> only
upload once; choose fresh IDs for any subsequent test.
"""
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

FIELDS = (
    "event_id", "trade_id", "event_type", "source_sequence",
    "client_id", "account_id", "instrument_id", "side",
    "quantity", "price", "trade_currency", "trade_date",
    "event_ts", "source_system",
)

# EVENT_ID, TRADE_ID, CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
# QUANTITY, PRICE, CURRENCY, EVENT_TIMESTAMP
BASE = [
    ("LABEVT202611110001","LABTRD202611110001","CL00016","AC000162","INS0015","BUY", "266",  "67.2881", "SGD","2026-11-11T13:00:00"),
    ("LABEVT202611110002","LABTRD202611110002","CL00023","AC000231","INS0009","BUY", "1170", "260.8622","INR","2026-11-11T13:01:00"),
    ("LABEVT202611110003","LABTRD202611110003","CL00011","AC000111","INS0024","SELL","193000","95.5317", "INR","2026-11-11T13:02:00"),
    ("LABEVT202611110004","LABTRD202611110004","CL00014","AC000141","INS0008","BUY", "171",  "115.8716","GBP","2026-11-11T13:03:00"),
    ("LABEVT202611110005","LABTRD202611110005","CL00024","AC000241","INS0011","SELL","386",  "188.9986","USD","2026-11-11T13:04:00"),
]

rows = []
for event_id,trade_id,client_id,account_id,instrument_id,side,qty,price,currency,ts in BASE:
    rows.append({
        "event_id": event_id,
        "trade_id": trade_id,
        "event_type": "I",
        "source_sequence": "1",
        "client_id": client_id,
        "account_id": account_id,
        "instrument_id": instrument_id,
        "side": side,
        "quantity": qty,
        "price": price,
        "trade_currency": currency,
        "trade_date": "2026-11-11",
        "event_ts": ts,
        "source_system": "SIM_OMS",
    })

update = dict(rows[0])
update.update(event_id="LABEVT202611110006", event_type="U", source_sequence="2",
              price="71.5000", event_ts="2026-11-11T13:07:00")
rows.append(update)

assert all(set(row) == set(FIELDS) for row in rows)
assert len({row["event_id"] for row in rows}) == 6
assert all(float(row["quantity"]) > 0 and float(row["price"]) > 0 for row in rows)

schema = pa.schema([pa.field(key, pa.string(), nullable=False) for key in FIELDS])
table = pa.Table.from_pylist(rows, schema=schema)
path = Path("trade_events_snowpipe_20261111_001.parquet")
pq.write_table(table, path, compression="snappy")

check = pq.read_table(path)
assert check.num_rows == 6 and check.column_names == list(FIELDS)
assert check.column("quantity").null_count == 0
print(f"Created {path}: {check.num_rows} rows, 14 columns, all qty values present.")
print("Upload ONCE to s3://omera-ib-trade-lake/landingzone/parquet/auto/")
