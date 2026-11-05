# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""How a run that publishes nothing says why: the contract refused it (``ContractViolation``), or the check could not be made
(``CheckNotRun``)."""


class ContractViolation(PermissionError):
    """The contract refused a read, a write or the run as a whole, so nothing the run wrote is published.

    ``violations`` lists the kinds of violation (``OUTPUT_SCHEMA_MISMATCH``, ``UNDECLARED_OUTPUT``, ...); the message is
    Invaract's standard explanation: what the contract expects, what the job did, why that violates it, and how to correct it.
    Left uncaught, it ends the job with exit status 1.
    """

    exit_status = 1

    def __init__(self, violations, explanation):
        super().__init__(explanation)
        self.violations = list(violations)
        self.explanation = explanation

    def add_notes(self, heading, notes):
        """Adds ``notes`` under ``heading`` to the explanation: what the package knows of the job that the checker does not
        (how pandas held a column it could not write as declared)."""
        self.explanation = self.explanation.rstrip("\n") + f"\n\n{heading}:\n" + "".join(f"  - {n}\n" for n in notes)
        self.args = (self.explanation,)


class CheckNotRun(RuntimeError):
    """The check could not be made: no checker to run, a checker that stopped, a contract it could not read, or a setting it
    does not know. The run's data is never published unchecked. Left uncaught, it ends the job with exit status 2."""

    exit_status = 2
