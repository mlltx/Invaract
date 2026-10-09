import importlib.machinery
import importlib.util
import json
import os
import re
import shutil
import tempfile
import unittest

import check_modules as c

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def load_generator():
    path = os.path.join(REPO, "dev", "new-adapter")
    loader = importlib.machinery.SourceFileLoader("new_adapter", path)
    spec = importlib.util.spec_from_loader("new_adapter", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


na = load_generator()


class Generated(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.out = tempfile.mkdtemp()
        cls.name = na.generate(REPO, "demo", "Demo", cls.out)
        cls.module = os.path.join(cls.out, cls.name)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.out)

    def files(self):
        return sorted(os.path.relpath(os.path.join(b, f), self.module) for b, _, fs in os.walk(self.module) for f in fs)

    def read(self, rel):
        with open(os.path.join(self.module, rel)) as f:
            return f.read()

    def test_the_module_is_named_and_laid_out_for_the_engine(self):
        self.assertEqual(self.name, "demo-adapter")
        files = self.files()
        for expected in [
            "build.sbt",
            "src/main/resources/invaract-capabilities-demo.yaml",
            "src/main/scala/com/invaract/demoadapter/DemoCapabilities.scala",
            "src/test/scala/com/invaract/demoadapter/DemoConformanceAdapter.scala",
            "src/test/scala/com/invaract/demoadapter/DemoConformanceSpec.scala",
            "src/test/scala/com/invaract/demoadapter/DemoAttestedClaimsSpec.scala",
            "src/test/scala/com/invaract/demoadapter/DemoCapabilitiesSpec.scala",
            "project/build.properties",
        ]:
            self.assertIn(expected, files)

    def test_no_placeholder_survives_in_any_file(self):
        for rel in self.files():
            self.assertEqual(re.findall(r"@@[A-Za-z_]+@@", self.read(rel)), [], rel)

    def test_sibling_versions_are_the_repositorys_current_ones(self):
        sbt = self.read("build.sbt")
        for module in ("contract", "ir", "verification-core"):
            self.assertIn(f'"invaract-{module}" % "{na.version_of(REPO, module)}"', sbt)
        self.assertIn(f'"invaract-adapter-testkit" % "{na.version_of(REPO, "adapter-testkit")}" % "test"', sbt)

    def test_the_generated_build_passes_the_registry_pin_check(self):
        # the registry check reads <root>/<module>/build.sbt for each registered module; lay the new one next to the real ones
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        for m in ("contract", "ir", "verification-core", "adapter-testkit"):
            os.makedirs(os.path.join(root, m))
            shutil.copy(os.path.join(REPO, m, "build.sbt"), os.path.join(root, m, "build.sbt"))
        shutil.copytree(self.module, os.path.join(root, self.name))
        modules = [{"name": m} for m in ("contract", "ir", "verification-core", "adapter-testkit")] + [{"name": self.name}]
        self.assertEqual(c.check_sibling_pins(root, modules), [])
        self.assertEqual(c.check_adapter_uses_kit(root, [{"name": self.name}]), [])
        self.assertEqual(c.check_adapter(root, {"name": self.name, "role": "adapter", "capabilities": "invaract-capabilities-demo.yaml"}), [])

    def test_every_capability_is_declared_and_unsupported_with_a_note(self):
        declaration = self.read("src/main/resources/invaract-capabilities-demo.yaml")
        ids = na.capability_ids(REPO)
        with open(os.path.join(REPO, "verification-core", "src", "main", "scala", "com", "invaract", "verification", "Capability.scala")) as f:
            vocabulary = re.findall(r'= Capability\("([^"]+)"', f.read())
        self.assertEqual(sorted(ids), sorted(vocabulary), "the generator must declare exactly the capabilities the vocabulary has")
        for cap in ids:
            self.assertRegex(declaration, rf"  {re.escape(cap)}:\n    status: unsupported\n    note: Not yet investigated for Demo\.")
        self.assertIn("point: observe-only", declaration)

    def test_the_publishing_block_is_taken_from_the_kit(self):
        sbt = self.read("build.sbt")
        self.assertIn("publishMavenStyle := true", sbt)
        self.assertIn("sonatypePublishToBundle", sbt)
        self.assertNotIn("invaract-adapter-testkit\"\n", sbt.split("libraryDependencies")[0])

    def doc(self):
        with open(os.path.join(self.out, "docs", "DEMO_ADAPTER.md")) as f:
            return f.read()

    def test_the_write_up_is_generated_with_every_capability_in_its_ledger(self):
        doc = self.doc()
        self.assertTrue(doc.startswith("# Demo Adapter\n"))
        for heading in c.DOC_HEADINGS:
            self.assertIn(heading, doc)
        for cap in na.capability_ids(REPO):
            self.assertIn(f"| `{cap}` | unsupported | TODO |", doc)
        self.assertEqual(len([l for l in doc.splitlines() if re.match(r"\| \d+ \|", l)]), 12, "the twelve investigation questions")

    def test_the_generated_write_up_passes_the_check_while_in_progress_and_fails_it_once_complete(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        os.makedirs(os.path.join(root, "docs"))
        shutil.copy(os.path.join(self.out, "docs", "DEMO_ADAPTER.md"), os.path.join(root, "docs"))
        shutil.copytree(self.module, os.path.join(root, self.name))
        module = {"name": self.name, "role": "adapter", "capabilities": "invaract-capabilities-demo.yaml",
                  "doc": "docs/DEMO_ADAPTER.md", "docStandard": True, "status": "in-progress"}
        sync = "src: 'docs/DEMO_ADAPTER.md',"
        self.assertEqual(c.check_adapter_doc(root, module, sync), [])
        found = c.check_adapter_doc(root, dict(module, status="complete"), sync)
        self.assertEqual(len(found), 2, "both the TODO and the not-investigated mark")
        self.assertIn("still has 'TODO'", found[0])
        self.assertIn("still has '\u2753'", found[1])

    def test_generating_over_an_existing_module_is_refused(self):
        with self.assertRaises(SystemExit):
            na.generate(REPO, "demo", "Demo", self.out)


class Registration(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        shutil.copy(os.path.join(REPO, "modules.json"), self.root)
        os.makedirs(os.path.join(self.root, "contributor-docs", "scripts"))
        shutil.copy(os.path.join(REPO, "contributor-docs", "scripts", "sync-docs.mjs"), os.path.join(self.root, "contributor-docs", "scripts"))

    def modules(self):
        with open(os.path.join(self.root, "modules.json")) as f:
            return json.load(f)["modules"]

    def test_a_new_adapter_is_registered_after_the_last_published_module(self):
        na.register(self.root, "demo", "Demo")
        modules = self.modules()
        names = [m["name"] for m in modules]
        self.assertEqual(names[names.index("spark-adapter") + 1], "demo-adapter")
        entry = modules[names.index("demo-adapter")]
        self.assertEqual(entry["role"], "adapter")
        self.assertTrue(entry["published"] and entry["mutation"])
        self.assertEqual(entry["capabilities"], "invaract-capabilities-demo.yaml")

    def test_registration_records_the_write_up_and_renders_it_in_the_contributor_docs(self):
        na.register(self.root, "demo", "Demo")
        entry = [m for m in self.modules() if m["name"] == "demo-adapter"][0]
        self.assertEqual(entry["doc"], "docs/DEMO_ADAPTER.md")
        self.assertTrue(entry["docStandard"])
        self.assertEqual(entry["status"], "in-progress")
        with open(os.path.join(self.root, "contributor-docs", "scripts", "sync-docs.mjs")) as f:
            sync = f.read()
        self.assertIn("src: 'docs/DEMO_ADAPTER.md',\n    slug: 'design/demo-adapter',\n    section: 'Design Docs',\n    label: 'Demo Adapter',", sync)
        # after the guide (order 8), before the Connectors section
        self.assertIn("order: 9,", sync.split("src: 'docs/DEMO_ADAPTER.md'", 1)[1].split("},", 1)[0])
        self.assertLess(sync.index("DEMO_ADAPTER.md"), sync.index("docs/connectors/delta.md"))

    def test_registering_twice_is_refused(self):
        na.register(self.root, "demo", "Demo")
        with self.assertRaises(SystemExit):
            na.register(self.root, "demo", "Demo")

    def test_the_rewritten_registry_keeps_its_comment_and_stays_valid_json(self):
        na.register(self.root, "demo", "Demo")
        with open(os.path.join(self.root, "modules.json")) as f:
            data = json.load(f)
        self.assertIn("_comment", data)


class Arguments(unittest.TestCase):
    def test_a_bad_engine_name_is_refused(self):
        for bad in ("Beam", "1beam", "big-query", ""):
            with self.assertRaises(SystemExit, msg=bad):
                na.main(["new-adapter", bad, "--out", tempfile.mkdtemp()])

    def test_a_bad_display_name_is_refused(self):
        with self.assertRaises(SystemExit):
            na.main(["new-adapter", "demo", "--display", "demo", "--out", tempfile.mkdtemp()])


if __name__ == "__main__":
    unittest.main()
