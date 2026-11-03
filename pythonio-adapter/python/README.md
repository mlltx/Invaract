# invaract

Reads and writes a Python job's data by the names its [Invaract](https://github.com/mlltx/Invaract) data contract gives,
and publishes what the job writes only if the contract allows it.

```bash
pip install "invaract[pandas]"     # or invaract[polars], or invaract for pyarrow alone
```

The guide: [Use Invaract with Python Jobs](https://mlltx.github.io/Invaract/guides/using-invaract-with-python/).

```python
import invaract

@invaract.job("contract.yaml")                    # INVARACT_CONTRACT overrides it
def main():
    orders = invaract.read("orders")              # pandas by default; as_="polars", "polars-lazy" or "arrow"
    invaract.write(report(orders), "report")      # checked now, staged, published when the run ends
```

The contract holds the locations, formats, save modes and columns; the job holds only names. Each read is checked against
the contract as it is made, and each write before anything is staged and again once it is (a required column must hold no
empty value). When the job returns, every staged output is published, or none if anything was refused. A refusal raises
`invaract.ContractViolation` (exit status 1); a check that could not be made raises `invaract.CheckNotRun` (exit status 2).

`invaract.write` takes anything that offers an Arrow stream: a pandas, Polars or DuckDB frame, a Polars LazyFrame (run
batch by batch into staging), a pyarrow Table or RecordBatchReader. A pandas frame's row index is not written.

**The checker.** The checks are Invaract's own, made by its checker program. The package starts it once per run and
describes each read and write to it (names, columns, types, empty values; never the data). The wheels for Linux (x86_64,
aarch64; glibc 2.34 or later) and macOS 11 or later (arm64, x86_64) carry it as a native program. On any other platform the wheel carries it as a Java
program, which needs Java 11 or later (`JAVA_HOME`, or `java` on the `PATH`). `INVARACT_CHECKER` names a checker of your
own instead: a native program, or `java -jar invaract-pythonio-checker-<version>.jar`. A source checkout carries none, so
it needs `INVARACT_CHECKER` (`cd pythonio-adapter && sbt assembly` builds the jar).

**Settings** come from the environment, so a platform can configure a job it does not own: `INVARACT_CONTRACT`,
`INVARACT_DRY_RUN`, `INVARACT_NOTIFY_CONFIG`, `INVARACT_LOCATION_MAP`, `INVARACT_ORG_POLICY`, `INVARACT_REGISTRY_URL` and
the rest of `invaract/_settings.py`.

**Migrating a job with no contract yet.** Replace each read and write, keeping its path as `location=`
(`invaract.read("orders", location="data/in/orders")`), and run it with `INVARACT_DRY_RUN=true`: the job reads and writes as
before, and a draft contract is published to the notification sink. Review it, commit it, and remove the `location=`
arguments. With a contract in force, a `location=` that disagrees with it is refused.

`examples/orders_job` is a job over three files (`load.py`, `build.py`, `main.py`) written this way. The design is
`docs/PYTHON_IO_ADAPTER_DESIGN.md`, the checker's side `docs/PYTHONIO_ADAPTER.md`. Tests: `python -m pytest` here, with a
checker built (`INVARACT_TEST_CHECKER`, or the jar `sbt assembly` builds in `pythonio-adapter`).
