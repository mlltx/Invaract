---
title: Configuration Settings
description: Every setting that turns Invaract on or changes what it checks, with its spelling on Spark, on Beam Java and on Beam Python.
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

On Spark, Invaract itself is installed with
`--conf spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension`; on
Beam, by the `--runner` above. The settings below then apply. Every setting is optional.
Settings that switch something on take `true` or `false`, and are off unless set to `true`.

## Choosing what to enforce

| Setting | Spark | Beam Java | Beam Python | What it does | Guide |
|---|---|---|---|---|---|
| Contract | `spark.invaract.contract` | `--invaractContract` | `--invaract_contract` | The contract to enforce: a file path, or `registry://<id>@<version>`. | [Install on Spark](/guides/installing-the-enforcement-rule/) |
| Dry run | `spark.invaract.dryRun` | `--invaractDryRun` | `--invaract_dry_run` | `true`: block nothing; infer a draft contract from each write instead. Needs no contract. | [Dry-run mode](/guides/dry-run-mode/) |
| Registry URL | `spark.invaract.registryUrl` | `--invaractRegistryUrl` | `--invaract_registry_url` | Where a `registry://` contract is fetched from. | [Use a contract registry](/guides/using-a-contract-registry/) |
| Registry client | `spark.invaract.registryClientClass` | `--invaractRegistryClientClass` | `--invaract_registry_client_class` | Class name of a registry client to use instead of the default one. | [Use a contract registry](/guides/using-a-contract-registry/) |
| Location map | `spark.invaract.locationMap` | `--invaractLocationMap` | `--invaract_location_map` | A `.properties` file mapping each `ref://<id>` location in the contract to a real path or table. | [Resolve dataset locations](/guides/resolving-dataset-locations/) |
| Organizational policy | `spark.invaract.orgPolicy` | `--invaractOrgPolicy` | `--invaract_org_policy` | A policy document checked against the contract before any write. | [Enforce an organizational policy](/guides/enforcing-organizational-policy/) |
| Policy overlays | `spark.invaract.orgPolicyOverlays` | `--invaractOrgPolicyOverlays` | `--invaract_org_policy_overlays` | Further policy layers, comma-separated, applied on top of `orgPolicy`. | [Enforce an organizational policy](/guides/enforcing-organizational-policy/) |

## Making the checks stricter

| Setting | Spark | Beam Java | Beam Python | What it does | Guide |
|---|---|---|---|---|---|
| Reject undeclared inputs | `spark.invaract.rejectUndeclaredInputs` | `--invaractRejectUndeclaredInputs` | `--invaract_reject_undeclared_inputs` | `true`: a read the contract doesn't declare blocks the write (`UNDECLARED_INPUT`). | [Violation types](/reference/violation-types/) |
| Reject undeclared fields | `spark.invaract.rejectUndeclaredFields` | `--invaractRejectUndeclaredFields` | `--invaract_reject_undeclared_fields` | `true`: an output column the contract doesn't declare blocks the write (`UNDECLARED_FIELD`). | [Violation types](/reference/violation-types/) |
| Static data quality | `spark.invaract.staticDataQuality` | `--invaractStaticDataQuality` | `--invaract_static_data_quality` | `true`: prove the contract's value constraints from the job's logic. | [Verify static data quality](/guides/verifying-static-data-quality/) |
| Role consistency | `spark.invaract.roleConsistency` | `--invaractRoleConsistency` | `--invaract_role_consistency` | `true`: check that each dataset is used the way its declared role says. | [Declare input and output types](/guides/declaring-input-and-output-types/) |
| Fingerprint | `spark.invaract.computeFingerprint` | `--invaractComputeFingerprint` | `--invaract_compute_fingerprint` | `true`: add a fingerprint of the transformation's logic to each validation event. | [Fingerprint a transformation](/guides/fingerprinting-transformations/) |
| Trusted steps | — | `--invaractTrustedSteps` | `--invaract_trusted_steps` | Comma-separated names of steps that end the pipeline without writing governed data, such as a metrics step. `*` matches any run of characters. Such a step is let through instead of refused. | [Beam Java](/guides/using-invaract-with-apache-beam/) |
| Fail on missing output | `spark.invaract.failOnMissingOutput` | — | — | `true`: a job that ends without writing an output its contract declares exits with a failure status. | [Install on Spark](/guides/installing-the-enforcement-rule/#every-declared-output-must-be-written) |

On Beam, static data quality, role consistency and fingerprinting are not supported yet.
If you turn one on, the pipeline is refused before it runs; it is never run with the check
silently skipped. Beam doesn't need `failOnMissingOutput`, because it sees the whole
pipeline before it runs: a missing output refuses the pipeline up front (`MISSING_OUTPUT`).
See [Engine Capabilities](/reference/engine-capabilities/) for what each engine can check.

## Reporting

| Setting | Spark | Beam Java | Beam Python | What it does | Guide |
|---|---|---|---|---|---|
| Notification config | `spark.invaract.notifyConfig` | `--invaractNotifyConfig` | `--invaract_notify_config` | A `.properties` file naming where validation events are sent. | [Configure a notification sink](/guides/notification-sinks/) |
| Job id | `spark.invaract.jobId` | — | — | A stable name for the job in every event (`job.jobId`). On Beam, events name the job by the pipeline's own `--jobName` / `--job_name`. | [Dry-run mode](/guides/dry-run-mode/) |
| Job metadata | `spark.invaract.job.metadata.<key>` | — | — | Free-form `key=value` pairs copied into every event's `job.attributes` (a team, an orchestrator's run id). | [Dry-run mode](/guides/dry-run-mode/) |
| Contract metadata | `spark.invaract.contract.metadata.<key>` | — | — | Free-form `key=value` pairs written into a dry-run draft's `extensions` (an owner, a source system). | [Dry-run mode](/guides/dry-run-mode/) |

## Beam-only settings

These control the Beam wrapper runner itself, not what Invaract checks.

| Beam Java | Beam Python | What it does |
|---|---|---|
| `--invaractDelegate` | `--invaract_delegate` | The runner the pipeline is handed to once it passes, for example `DataflowRunner`. Default: `DirectRunner`. |
| `--invaractCheckOnly` | `--invaract_check_only` | `true`: check the pipeline (or, in dry-run mode, infer its contract) and stop. Nothing runs, so a CI build needs no data, cluster or credentials. |
| — | `--invaract_checker_jar` | Path of the checker jar. Default: the `INVARACT_BEAM_CHECKER_JAR` environment variable. |
| — | `--invaract_java` | The `java` program that runs the checker. Default: `$JAVA_HOME/bin/java`, else `java` on the `PATH`. |
| — | `--invaract_java_options` | Options for the checker's JVM, for example `-Xmx512m`. |
