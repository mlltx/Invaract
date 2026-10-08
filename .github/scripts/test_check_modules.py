import json
import os
import shutil
import tempfile
import unittest

import check_modules as c

REAL_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

MODULES = [
    {"name": "core", "role": "engine", "published": True, "mutation": False},
    {"name": "kit", "role": "kit", "published": True, "mutation": True},
    {"name": "eng-adapter", "role": "adapter", "published": True, "mutation": True, "capabilities": "invaract-capabilities-eng.yaml"},
    {"name": "harness", "role": "harness", "published": False, "mutation": False},
    {"name": "ext", "role": "extension", "published": False, "mutation": False, "built": False},
]
PUBLISHED = ["core", "kit", "eng-adapter"]
EVERYTHING = [m["name"] for m in MODULES]
BUILT = ["core", "kit", "eng-adapter", "harness"]


class Loops(unittest.TestCase):
    def test_matching_loop_is_clean(self):
        self.assertEqual(c.check_for_loops("t.yml", "run: |\n  for module in core kit eng-adapter; do\n", PUBLISHED), [])

    def test_missing_module_is_named_with_its_line(self):
        found = c.check_for_loops("t.yml", "a\nb\n  for module in core kit; do\n", PUBLISHED)
        self.assertEqual(len(found), 1)
        self.assertIn("t.yml:3", found[0])
        self.assertIn("missing eng-adapter", found[0])

    def test_unknown_module_is_named(self):
        found = c.check_for_loops("t.yml", "for module in core kit eng-adapter other; do", PUBLISHED)
        self.assertIn("unknown other", found[0])

    def test_wrong_order_is_reported_as_order(self):
        found = c.check_for_loops("t.yml", "for module in kit core eng-adapter; do", PUBLISHED)
        self.assertIn("order differs", found[0])

    def test_every_loop_in_the_file_is_checked(self):
        text = "for module in core kit eng-adapter; do\nfor module in core; do\n"
        self.assertEqual(len(c.check_for_loops("t.yml", text, PUBLISHED)), 1)

    def test_a_file_with_no_loop_is_reported(self):
        self.assertEqual(len(c.check_for_loops("t.yml", "nothing here", PUBLISHED)), 1)


class Filters(unittest.TestCase):
    JOB = "  other:\n    x: 1\n  changes:\n    steps:\n      run: git diff --name-only \"$BASE\" HEAD -- core kit eng-adapter .github\n  next:\n    y: 2\n"

    def test_watching_every_published_module_is_clean(self):
        self.assertEqual(c.check_changes_filter("t.yml", self.JOB, PUBLISHED), [])

    def test_a_module_the_filter_does_not_watch_is_named(self):
        found = c.check_changes_filter("t.yml", self.JOB.replace("eng-adapter ", ""), PUBLISHED)
        self.assertIn("does not watch eng-adapter", found[0])

    def test_a_diff_in_another_job_is_not_mistaken_for_the_filter(self):
        text = "  mutation:\n    run: git diff --name-only a HEAD -- core kit eng-adapter\n  changes:\n    run: git diff --name-only a HEAD -- core\n"
        self.assertIn("does not watch kit", c.check_changes_filter("t.yml", text, PUBLISHED)[0])


class Releases(unittest.TestCase):
    def test_steps_in_order_are_clean(self):
        text = "- name: Publish core\n- name: Publish kit\n- name: Publish eng-adapter\n"
        self.assertEqual(c.check_release_steps("r.yml", text, PUBLISHED), [])

    def test_a_missing_publish_step_is_reported(self):
        found = c.check_release_steps("r.yml", "- name: Publish core\n- name: Publish kit\n", PUBLISHED)
        self.assertIn("missing eng-adapter", found[0])


class Lists(unittest.TestCase):
    def test_build_order_accepts_a_module_started_on_two_branches(self):
        text = "  start_module core \"x\"\nstart_module core \"x\"\nstart_module kit \"x\"\nstart_module eng-adapter \"x\"\n  start_module harness \"y\"\n"
        self.assertEqual(c.check_build_order("dev/build", text, BUILT), [])

    def test_build_order_names_a_module_never_started(self):
        found = c.check_build_order("dev/build", "start_module core \"x\"\n", BUILT)
        self.assertIn("missing kit, eng-adapter, harness", found[0])

    def test_function_definitions_are_not_modules(self):
        text = 'start_module() {\n}\nstart_module core "x"\n'
        self.assertIn("missing", c.check_build_order("dev/build", text, ["core", "kit"])[0])

    def test_warm_list_matches_built_modules_only(self):
        text = 'warm_module() {\n}\nwarm_module "core" "x"\nwarm_module "kit" "x"\nwarm_module "eng-adapter" "x"\nwarm_module "harness" "x"\n'
        self.assertEqual(c.check_warm_list("s.sh", text, BUILT), [])
        self.assertIn("unknown ext", c.check_warm_list("s.sh", text + 'warm_module "ext" "x"\n', BUILT)[0])

    def test_matrix_must_list_every_module(self):
        ok = "matrix:\n  module: [core, kit, eng-adapter, harness, ext]\n"
        self.assertEqual(c.check_matrix("g.yml", ok, EVERYTHING), [])
        self.assertIn("missing ext", c.check_matrix("g.yml", ok.replace(", ext", ""), EVERYTHING)[0])
        self.assertEqual(len(c.check_matrix("g.yml", "nothing", EVERYTHING)), 1)


class MutationJobs(unittest.TestCase):
    TEXT = "jobs:\n  mutation-testing-kit:\n    x: 1\n  mutation-testing-eng-adapter:\n    x: 1\n  mutation-testing-eng-adapter-incremental:\n    x: 1\n  summary:\n    needs: [test, mutation-testing-kit, mutation-testing-eng-adapter, mutation-testing-eng-adapter-incremental]\n    steps: []\n"

    def test_jobs_present_and_awaited_is_clean(self):
        self.assertEqual(c.check_mutation_jobs("t.yml", self.TEXT, ["kit", "eng-adapter"]), [])

    def test_a_module_with_no_job_is_reported(self):
        found = c.check_mutation_jobs("t.yml", self.TEXT, ["kit", "eng-adapter", "core"])
        self.assertIn("no `mutation-testing-core` job", found[0])

    def test_a_job_the_summary_does_not_await_is_reported(self):
        found = c.check_mutation_jobs("t.yml", self.TEXT.replace("mutation-testing-kit, ", ""), ["kit"])
        self.assertIn("summary", found[0])


class Tree(unittest.TestCase):
    """Checks that read the module directories."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)

    def write(self, path, text):
        full = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)

    def sbt(self, name, version="1.0.0", deps=()):
        self.write(f"{name}/build.sbt", f'ThisBuild / version := "{version}"\n' + "\n".join(deps) + "\n")

    def test_a_directory_with_a_build_but_no_registration_is_reported(self):
        self.sbt("core")
        self.sbt("newcomer")
        found = c.check_unregistered(self.root, [{"name": "core"}])
        self.assertEqual(len(found), 1)
        self.assertIn("newcomer/", found[0])

    def test_a_registered_module_with_no_directory_is_reported(self):
        self.sbt("core")
        found = c.check_unregistered(self.root, [{"name": "core"}, {"name": "ghost"}])
        self.assertIn("ghost", found[0])

    def test_sibling_pin_must_match_current_version(self):
        self.sbt("core", "2.0.0")
        self.sbt("user", deps=['libraryDependencies += "com.invaract" %% "invaract-core" % "1.0.0"'])
        found = c.check_sibling_pins(self.root, [{"name": "core"}, {"name": "user"}])
        self.assertEqual(len(found), 1)
        self.assertIn("user/build.sbt:2", found[0])
        self.assertIn("core 1.0.0, but core is 2.0.0", found[0])

    def test_matching_pin_and_foreign_dependency_are_clean(self):
        self.sbt("core", "2.0.0")
        self.sbt("user", deps=['"com.invaract" %% "invaract-core" % "2.0.0"', '"com.invaract" %% "invaract-elsewhere" % "9.9.9"'])
        self.assertEqual(c.check_sibling_pins(self.root, [{"name": "core"}, {"name": "user"}]), [])

    def test_a_comparison_baseline_is_not_a_pin(self):
        self.sbt("core", "2.0.0")
        self.sbt("user", deps=['mimaPreviousArtifacts := Set("com.invaract" %% "invaract-core" % "1.0.0")'])
        self.assertEqual(c.check_sibling_pins(self.root, [{"name": "core"}, {"name": "user"}]), [])

    def test_adapter_needs_the_kit_as_a_test_dependency(self):
        self.sbt("eng-adapter", deps=['"com.invaract" %% "invaract-adapter-testkit" % "1.0.0" % "test"'])
        self.assertEqual(c.check_adapter_uses_kit(self.root, [{"name": "eng-adapter"}]), [])
        self.sbt("eng-adapter")
        self.assertEqual(len(c.check_adapter_uses_kit(self.root, [{"name": "eng-adapter"}])), 1)

    def adapter(self, declaration=True, conformance=True, attested=True):
        if declaration:
            self.write("eng-adapter/src/main/resources/invaract-capabilities-eng.yaml", "adapter: eng\n")
        if conformance:
            self.write("eng-adapter/src/test/scala/A.scala", "class EngConformanceSpec extends AdapterConformanceSpec {}\n")
        if attested:
            self.write("eng-adapter/src/test/scala/B.scala", "class EngAttestedClaimsSpec extends AttestedClaimsSpec {}\n")

    def test_a_complete_adapter_is_clean(self):
        self.adapter()
        self.assertEqual(c.check_adapter(self.root, MODULES[2]), [])

    def test_each_missing_part_of_an_adapter_is_reported(self):
        self.adapter(declaration=False, conformance=False, attested=False)
        found = c.check_adapter(self.root, MODULES[2])
        self.assertEqual(len(found), 3)
        self.assertTrue(any("capability declaration" in f for f in found))
        self.assertTrue(any("AdapterConformanceSpec" in f for f in found))
        self.assertTrue(any("AttestedClaimsSpec" in f for f in found))

    def test_an_adapter_with_no_declared_file_name_is_reported(self):
        self.adapter()
        found = c.check_adapter(self.root, {"name": "eng-adapter", "role": "adapter"})
        self.assertEqual(len(found), 1)


class RealRepository(unittest.TestCase):
    def test_the_registry_agrees_with_every_file_that_lists_modules(self):
        self.assertEqual(c.check_all(REAL_ROOT), [])

    def test_the_registry_is_well_formed(self):
        with open(os.path.join(REAL_ROOT, "modules.json")) as f:
            modules = json.load(f)["modules"]
        names = [m["name"] for m in modules]
        self.assertEqual(len(names), len(set(names)))
        for m in modules:
            self.assertIn(m["role"], {"engine", "kit", "adapter", "harness", "extension"})
            self.assertIn("published", m)
            self.assertIn("mutation", m)
        self.assertEqual([m["role"] for m in modules if m["role"] == "kit"], ["kit"])
        # published modules come first, in dependency order: the loops that read this list rely on it
        flags = [m["published"] for m in modules]
        self.assertEqual(flags, sorted(flags, reverse=True))


if __name__ == "__main__":
    unittest.main()
