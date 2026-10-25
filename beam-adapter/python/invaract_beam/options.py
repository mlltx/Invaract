# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Invaract's flags, as options of a Beam pipeline: ``--invaract_<setting>=value`` on the job's command line.

The settings are Invaract's engine-neutral ones (the Java checker reads them; see docs/BEAM_PYTHON_ADAPTER.md), spelled
the way Python pipeline options are. This package does not interpret them: each one given is forwarded to the checker
unchanged, so a setting means the same here as on every other engine. ``SETTINGS`` must name every setting the checker
reads; beam-adapter's PortableCheckSpec fails, naming this file, when the two differ.
"""
from apache_beam.options.pipeline_options import PipelineOptions

# The engine-neutral settings, in the checker's spelling after the ``--invaract_`` prefix, each with its help text.
SETTINGS = {
    "contract": "The data contract to enforce: a file path, or registry://<id>@<version>.",
    "dry_run": "true: observe and infer a contract from the job's writes instead of enforcing one; needs no contract.",
    "notify_config": "Path of a notification-sink .properties file: where validation events are sent.",
    "registry_url": "Base URL of the contract registry a registry:// contract is fetched from.",
    "registry_client_class": "Class name of the registry client the checker loads, overriding the default.",
    "location_map": "Path of an id=location .properties file that ref://<id> locations in the contract resolve against.",
    "org_policy": "Path of an organizational policy document layered on the contract.",
    "org_policy_overlays": "Comma-separated paths of further organizational policy layers.",
    "reject_undeclared_inputs": "true: a read of a dataset the contract does not declare is blocked.",
    "reject_undeclared_fields": "true: an output column the contract does not declare is blocked.",
    "compute_fingerprint": "true: compute a semantic fingerprint (declared unsupported for Python pipelines, so none is computed and a warning says so).",
    "static_data_quality": "true: prove the contract's value constraints (declared unsupported for Python pipelines, so a run with it is refused).",
    "role_consistency": "true: check each input's declared role (declared unsupported for Python pipelines, so a run with it is refused).",
    "trusted_steps": "Comma-separated names of steps the platform trusts not to move governed data (* for any run of characters).",
}


class InvaractOptions(PipelineOptions):
    """The options ``InvaractRunner`` reads: where the checked pipeline goes next, how to run the checker, and the settings."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--invaract_delegate", default="DirectRunner",
            help="The runner the pipeline is handed to once it passes: a runner name (DataflowRunner) or module.Class. Default: DirectRunner.")
        parser.add_argument(
            "--invaract_check_only", default=None,
            help="true: check the pipeline (or, with --invaract_dry_run, infer its contract) and stop there: it is handed to no runner, "
                 "so a CI build needs no data, cluster or credentials for it.")
        parser.add_argument(
            "--invaract_checker_jar", default=None,
            help="Path of the Invaract checker jar. Default: the INVARACT_BEAM_CHECKER_JAR environment variable.")
        parser.add_argument(
            "--invaract_java", default=None,
            help="The java program that runs the checker. Default: $JAVA_HOME/bin/java, else java on the PATH.")
        parser.add_argument(
            "--invaract_java_options", default=None,
            help="Options for the checker's JVM (-Xmx512m). With no checker jar, these must give it a classpath.")
        for name, description in SETTINGS.items():
            parser.add_argument("--invaract_" + name, default=None, help=description)
