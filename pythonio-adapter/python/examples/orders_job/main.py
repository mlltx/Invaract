# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""The example job's entry point: one run per call. Run it from this folder, with INVARACT_CHECKER naming the checker:

    INVARACT_CHECKER="java -jar invaract-pythonio-checker-0.1.0.jar" python main.py

It writes data/out/report and data/out/audit only if both pass the contract, and exits 1 (refused) or 2 (not checked)
otherwise, publishing nothing.
"""
import build
import invaract
import load


@invaract.job("contract.yaml")
def main():
    paid = load.paid_orders()
    build.audit(paid)
    invaract.write(build.report(paid, load.customers()), "report")  # was report.to_parquet("data/out/report/part-0.parquet")


if __name__ == "__main__":
    main()
