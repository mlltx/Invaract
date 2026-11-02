# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Where the example job reads. Each read was a pandas call with a path in it; now it names the input."""
import invaract


def paid_orders():
    orders = invaract.read("orders")  # was pd.read_parquet("data/in/orders")
    return orders[orders["status"] == "paid"]


@invaract.reads("customers")  # was pd.read_csv("data/in/customers.csv")
def customers(customers):
    return customers
