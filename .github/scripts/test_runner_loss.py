import unittest

import runner_loss as r

# Annotation payloads as observed on real runs (PR #92, "Test and Build"):
LOST = [{"message": "The operation was canceled.", "annotation_level": "failure"}]
SHUTDOWN = [{"message": "The runner has received a shutdown signal. This can happen when the runner service is stopped"}]
REAL = [{"message": "Process completed with exit code 1."}]


def job(name, conclusion, annotations=()):
    return {"name": name, "conclusion": conclusion, "annotations": list(annotations)}


class IsRunnerLoss(unittest.TestCase):
    def test_canceled_annotation(self):
        self.assertTrue(r.is_runner_loss(LOST))

    def test_shutdown_annotation(self):
        self.assertTrue(r.is_runner_loss(SHUTDOWN))

    def test_real_exit_code_is_not_runner_loss(self):
        self.assertFalse(r.is_runner_loss(REAL))

    def test_real_failure_wins_over_cancel_in_same_job(self):
        self.assertFalse(r.is_runner_loss(LOST + REAL))

    def test_no_annotations_is_not_runner_loss(self):
        self.assertFalse(r.is_runner_loss([]))

    def test_missing_message_tolerated(self):
        self.assertFalse(r.is_runner_loss([{"message": None}, {}]))


class Decide(unittest.TestCase):
    gate = job("Test Summary", "failure")

    def test_only_runner_loss_reruns(self):
        rerun, why = r.decide(1, [job("a", "success"), job("b", "failure", LOST), job("c", "failure", SHUTDOWN), self.gate])
        self.assertTrue(rerun)
        self.assertIn("b, c", why)

    def test_one_real_failure_blocks_rerun(self):
        rerun, why = r.decide(1, [job("b", "failure", LOST), job("c", "failure", REAL), self.gate])
        self.assertFalse(rerun)
        self.assertIn("c", why)

    def test_second_attempt_is_not_retried(self):
        self.assertFalse(r.decide(2, [job("b", "failure", LOST)])[0])

    def test_first_attempt_boundary(self):
        self.assertTrue(r.decide(r.MAX_ATTEMPTS - 1, [job("b", "failure", LOST)])[0])

    def test_gate_alone_failing_is_not_runner_loss(self):
        self.assertFalse(r.decide(1, [job("a", "success"), self.gate])[0])

    def test_cancelled_jobs_are_ignored(self):
        self.assertTrue(r.decide(1, [job("a", "cancelled"), job("b", "failure", LOST)])[0])


if __name__ == "__main__":
    unittest.main()
