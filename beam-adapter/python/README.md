# invaract-beam

Checks an Apache Beam pipeline written in Python against an [Invaract](https://github.com/mlltx/Invaract) data contract
before it is submitted, with no change to the job:

```bash
pip install "git+https://github.com/mlltx/Invaract#subdirectory=beam-adapter/python"
export INVARACT_BEAM_CHECKER_JAR=/path/to/invaract-beam-checker-0.1.0.jar   # cd beam-adapter && sbt assembly
python my_job.py --runner=invaract_beam.InvaractRunner --invaract_delegate=DataflowRunner \
    --invaract_contract=contracts/orders_report.yaml ...
```

The runner writes the pipeline's portable form to a file and runs Invaract's checker on it (a Java program: Java 11 or later
must be installed where the job is submitted). A pipeline its contract refuses raises `invaract_beam.ContractViolation`; one
that could not be checked raises `invaract_beam.CheckNotRun`. Neither is submitted.

This package only bridges to the checker: every decision is made there. The user guide is the docs site's "Use Invaract with
Beam Python Pipelines"; the design is `docs/BEAM_PYTHON_ADAPTER.md`. Tests: `python -m pytest` here, and beam-adapter's
`BeamPython*` suites for the end to end runs.
