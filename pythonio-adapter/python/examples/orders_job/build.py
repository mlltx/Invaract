# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The example job's transformations: unchanged by adopting Invaract, except that the audit is written by name."""
import invaract


def report(paid, customers):
    return paid.merge(customers, on="customer_id").groupby("region", as_index=False).agg(total=("amount", "sum"))


@invaract.writes("audit")  # was audit.to_parquet("data/out/audit/part-0.parquet")
def audit(paid):
    return paid[["order_id", "status"]]
