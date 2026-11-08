---
title: Configuration Settings
description: Every setting that turns Invaract on or changes what it checks, with its spelling on Spark, on Beam Java, on Beam Python and for Python jobs.
sidebar:
  order: 1.5
---

Every setting on this page can be set where the job is launched, without touching the job's
source code. That lets a platform team attach Invaract to a job it doesn't own. The
settings have the same meaning on every engine. Each engine spells them its own way:

| Engine | How a setting is spelled | Where it goes |
|---|---|---|
| Spark | `spark.invaract.<name>` | `spark-submit --conf spark.invaract.<name>=<value>` (or `spark-defaults.conf`) |
| Beam Java | `--invaract<Name>` | A pipeline option on the job's command line, next to `--runner=Invaract` |
| Beam Python | `--invaract_<snake_name>` | A pipeline option on the job's command line, next to `--runner=invaract_beam.InvaractRunner` |
| Python | `INVARACT_<SNAKE_NAME>` | An environment variable of the job's process |

On Spark, Invaract itself is installed with
`--conf spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension`; on
Beam, by the `--runner` above; a Python job, by reading and writing through the `invaract` package
([Use Invaract with Python Jobs](/guides/using-invaract-with-python/)). The settings below then apply. Every setting is optional.
Settings that switch something on take `true` or `false` (in any case), and are off unless
set to `true`. Any other value, such as `yes`, is refused when the job starts, with an error
naming the setting.

## Choosing what to enforce

| Setting | Spark | Beam Java | Beam Python | Python | What it does | Guide |
|---|---|---|---|---|---|---|
| Contract | `spark.invaract.contract` | `--invaractContract` | `--invaract_contract` | `INVARACT_CONTRACT` | The contract to enforce: a file path, or `registry://<id>@<version>`. | [Install on Spark](/guides/installing-the-enforcement-rule/) |
| Dry run | `spark.invaract.dryRun` | `--invaractDryRun` | `--invaract_dry_run` | `INVARACT_DRY_RUN` | `true`: block nothing; infer a draft contract from each write instead. Needs no contract. | [Dry-run mode](/guides/dry-run-mode/) |
| Registry URL | `spark.invaract.registryUrl` | `--invaractRegistryUrl` | `--invaract_registry_url` | `INVARACT_REGISTRY_URL` | Where a `registry://` contract is fetched from. | [Use a contract registry](/guides/using-a-contract-registry/) |
| Registry client | `spark.invaract.registryClientClass` | `--invaractRegistryClientClass` | `--invaract_registry_client_class` | `INVARACT_REGISTRY_CLIENT_CLASS` | Class name of a registry client to use instead of the default one. | [Use a contract registry](/guides/using-a-contract-registry/) |
| Location map | `spark.invaract.locationMap` | `--invaractLocationMap` | `--invaract_location_map` | `INVARACT_LOCATION_MAP` | A `.properties` file mapping each `ref://<id>` location in the contract to a real path or table. | [Resolve dataset locations](/guides/resolving-dataset-locations/) |
| Location service | `spark.invaract.locationUrl` | `--invaractLocationUrl` | `--invaract_location_url` | `INVARACT_LOCATION_URL` | Base URL of a service asked where each `ref://<id>` location is (`GET <url>/locations/<id>`), for the ids the location map does not hold. | [Resolve dataset locations](/guides/resolving-dataset-locations/#or-ask-a-location-service) |
| Location service token | `spark.invaract.locationTokenEnv` | `--invaractLocationTokenEnv` | `--invaract_location_token_env` | `INVARACT_LOCATION_TOKEN_ENV` | The name of the environment variable holding the location service's bearer token: the name, never the token. | [Resolve dataset locations](/guides/resolving-dataset-locations/#or-ask-a-location-service) |
| Organizational policy | `spark.invaract.orgPolicy` | `--invaractOrgPolicy` | `--invaract_org_policy` | `INVARACT_ORG_POLICY` | A policy document checked against the contract before any write. | [Enforce an organizational policy](/guides/enforcing-organizational-policy/) |
| Policy overlays | `spark.invaract.orgPolicyOverlays` | `--invaractOrgPolicyOverlays` | `--invaract_org_policy_overlays` | `INVARACT_ORG_POLICY_OVERLAYS` | Further policy layers, comma-separated, applied on top of `orgPolicy`. | [Enforce an organizational policy](/guides/enforcing-organizational-policy/) |

## Making the checks stricter

| Setting | Spark | Beam Java | Beam Python | Python | What it does | Guide |
|---|---|---|---|---|---|---|
| Reject undeclared inputs | `spark.invaract.rejectUndeclaredInputs` | `--invaractRejectUndeclaredInputs` | `--invaract_reject_undeclared_inputs` | `INVARACT_REJECT_UNDECLARED_INPUTS` | `true`: a read the contract doesn't declare blocks the write (`UNDECLARED_INPUT`). | [Violation types](/reference/violation-types/) |
| Reject undeclared fields | `spark.invaract.rejectUndeclaredFields` | `--invaractRejectUndeclaredFields` | `--invaract_reject_undeclared_fields` | `INVARACT_REJECT_UNDECLARED_FIELDS` | `true`: an output column the contract doesn't declare blocks the write (`UNDECLARED_FIELD`). | [Violation types](/reference/violation-types/) |
| Static data quality | `spark.invaract.staticDataQuality` | `--invaractStaticDataQuality` | `--invaract_static_data_quality` | `INVARACT_STATIC_DATA_QUALITY` | `true`: prove the contract's value constraints from the job's logic. | [Verify static data quality](/guides/verifying-static-data-quality/) |
| Runtime data quality | `spark.invaract.runtimeDataQuality` | `--invaractRuntimeDataQuality` | `--invaract_runtime_data_quality` | `INVARACT_RUNTIME_DATA_QUALITY` | `true`: check every row a job writes against its outputs' declared value constraints before publishing (Python jobs; Spark and Beam refuse constrained outputs). | [Check data values at run time](/guides/checking-data-values-at-run-time/) |
| Role consistency | `spark.invaract.roleConsistency` | `--invaractRoleConsistency` | `--invaract_role_consistency` | `INVARACT_ROLE_CONSISTENCY` | `true`: check that each dataset is used the way its declared role says. | [Declare input and output types](/guides/declaring-input-and-output-types/) |
| Fingerprint | `spark.invaract.computeFingerprint` | `--invaractComputeFingerprint` | `--invaract_compute_fingerprint` | `INVARACT_COMPUTE_FINGERPRINT` | `true`: add a fingerprint of the transformation's logic to each validation event. | [Fingerprint a transformation](/guides/fingerprinting-transformations/) |
| Trusted steps | — | `--invaractTrustedSteps` | `--invaract_trusted_steps` | — | Comma-separated names of steps that end the pipeline without writing governed data, such as a metrics step. `*` matches any run of characters. Such a step is let through instead of refused. | [Beam Java](/guides/using-invaract-with-apache-beam/) |
| Fail on missing output | `spark.invaract.failOnMissingOutput` | — | — | — | `true`: a job that ends without writing an output its contract declares exits with a failure status. | [Install on Spark](/guides/installing-the-enforcement-rule/#every-declared-output-must-be-written) |

On Beam, static data quality and role consistency are not supported yet. If you turn one on,
the pipeline is refused before it runs; it is never run with the check silently skipped.
Fingerprints aren't supported on Beam either, but a fingerprint is a report, not a check: with
`computeFingerprint` on, the pipeline runs and publishes no fingerprint, and a warning in the
log says why. Beam doesn't need `failOnMissingOutput`, because it sees the whole pipeline
before it runs: a missing output refuses the pipeline up front (`MISSING_OUTPUT`). See
[What Each Engine Checks](/reference/what-each-engine-checks/) for what each engine can do.

Python jobs are the same as Beam here: with static data quality or role consistency on, the run is refused when it
starts (`UNSUPPORTED_CONTRACT_FEATURE`), before the job reads anything; with `INVARACT_COMPUTE_FINGERPRINT=true` the job runs and a
warning says no fingerprint is published. A read of an input the contract doesn't declare is always refused there, since
every read names the input it reads. A declared output the job never wrote is refused when the job returns
(`MISSING_OUTPUT`), and then nothing is published.

## Reporting

| Setting | Spark | Beam Java | Beam Python | Python | What it does | Guide |
|---|---|---|---|---|---|---|
| Notification config | `spark.invaract.notifyConfig` | `--invaractNotifyConfig` | `--invaract_notify_config` | `INVARACT_NOTIFY_CONFIG` | A `.properties` file naming where validation events are sent. | [Configure a notification sink](/guides/notification-sinks/) |
| Job id | `spark.invaract.jobId` | `--invaractJobId` | `--invaract_job_id` | `INVARACT_JOB_ID` | A stable name for the job across its runs, in every event (`job.jobId`). Set it: the engine's own name for a run (Spark's app name, Beam's `--jobName`) often changes from run to run under a scheduler. | [Configure a notification sink](/guides/notification-sinks/) |
| Job attributes | `spark.invaract.jobAttributes` | `--invaractJobAttributes` | `--invaract_job_attributes` | `INVARACT_JOB_ATTRIBUTES` | Facts about this run, copied into every event's `job.attributes`: comma-separated `key=value` pairs, such as `team=orders,dag=orders_daily` (a value cannot hold a comma). Pairs that are not `key=value`, or a key given twice, are refused when the job starts. | [Configure a notification sink](/guides/notification-sinks/) |
| Job metadata | `spark.invaract.job.metadata.<key>` | — | — | — | One attribute per setting, the same as a `jobAttributes` pair; for a key set both ways, this one wins. | [Dry-run mode](/guides/dry-run-mode/) |
| Contract metadata | `spark.invaract.contractMetadata` | `--invaractContractMetadata` | `--invaract_contract_metadata` | `INVARACT_CONTRACT_METADATA` | Facts about the contract a dry run drafts, written into every draft's `extensions` and every dry-run event's `metadata`: comma-separated `key=value` pairs, such as `owner=orders-team,source=erp` (a value cannot hold a comma). Malformed pairs, or a key given twice, are refused when the job starts. | [Dry-run mode](/guides/dry-run-mode/#tag-the-draft-and-link-events-back-to-the-run) |
| Contract metadata, per key | `spark.invaract.contract.metadata.<key>` | — | — | — | One pair per setting, the same as a `contractMetadata` pair; for a key set both ways, this one wins. | [Dry-run mode](/guides/dry-run-mode/#tag-the-draft-and-link-events-back-to-the-run) |

## Beam-only settings

These control the Beam wrapper runner itself, not what Invaract checks.

| Beam Java | Beam Python | What it does |
|---|---|---|
| `--invaractDelegate` | `--invaract_delegate` | The runner the pipeline is handed to once it passes, for example `DataflowRunner`. Default: `DirectRunner`. |
| `--invaractCheckOnly` | `--invaract_check_only` | `true`: check the pipeline (or, in dry-run mode, infer its contract) and stop. Nothing runs, so a CI build needs no data, cluster or credentials. |
| — | `--invaract_checker_jar` | Path of the checker jar. Default: the `INVARACT_BEAM_CHECKER_JAR` environment variable. |
| — | `--invaract_java` | The `java` program that runs the checker. Default: `$JAVA_HOME/bin/java`, else `java` on the `PATH`. |
| — | `--invaract_java_options` | Options for the checker's JVM, for example `-Xmx512m`. |

## Python-only settings

| Environment variable | What it does |
|---|---|
| `INVARACT_CHECKER` | The command that runs the checker, split as a shell would: a native checker, or `java -jar <path>/invaract-pythonio-checker.jar`. Default: the checker the installed package carries (see [Use Invaract with Python Jobs](/guides/using-invaract-with-python/#install-the-package)). |
