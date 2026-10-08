#!/usr/bin/env python3
"""Fails when a file that hand-lists the repository's sbt modules disagrees with modules.json.

Adding an engine adapter used to mean remembering about a dozen places (the build order, the CI loops, the
release steps, the mutation job, the warm-up list ...), and forgetting one failed silently or late. The registry
is the one list; this check says, for every place that keeps its own, exactly which module is missing or extra
and where. It is deliberately a reader of those files, not a generator of them: each has its own syntax and its
own reasons, and a message that names the line is something a person or the add-an-adapter skill can act on.

Usage: check_modules.py [repo_root]      (exit 0 = consistent, 1 = drift, with one line per finding)
"""
import json
import os
import re
import sys

FOR_LOOP = re.compile(r"for module in ([a-z0-9 -]+?); do")


def load_registry(root):
    with open(os.path.join(root, "modules.json")) as f:
        return json.load(f)["modules"]


def read(root, path):
    with open(os.path.join(root, path)) as f:
        return f.read()


def line_of(text, needle):
    index = text.find(needle)
    return text.count("\n", 0, index) + 1 if index >= 0 else 1


def names(modules, **where):
    return [m["name"] for m in modules if all(m.get(k) == v for k, v in where.items())]


def diff_note(expected, actual):
    missing = [m for m in expected if m not in actual]
    extra = [m for m in actual if m not in expected]
    parts = []
    if missing:
        parts.append("missing " + ", ".join(missing))
    if extra:
        parts.append("unknown " + ", ".join(extra))
    if not parts:
        parts.append("order differs, expected " + " ".join(expected))
    return "; ".join(parts)


def check_for_loops(path, text, published):
    """Every `for module in a b c; do` loop lists the published modules, in dependency order."""
    found = []
    for match in FOR_LOOP.finditer(text):
        actual = match.group(1).split()
        if actual != published:
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{path}:{line}: `for module in` loop is out of step with modules.json ({diff_note(published, actual)})")
    if not FOR_LOOP.search(text):
        found.append(f"{path}: expected a `for module in ...; do` loop over the published modules and found none")
    return found


def check_changes_filter(path, text, published):
    """The engine-change filter diffs every published module's directory."""
    job = re.search(r"^  changes:\s*$.*?(?=^  [a-z0-9-]+:\s*$|\Z)", text, re.M | re.S)
    match = re.search(r"git diff --name-only [^\n]*? HEAD -- ([^\n]+)", job.group(0) if job else "")
    if job and match:
        text = text[: job.start()] + job.group(0)  # line numbers below count from the top of the file
    if not match:
        return [f"{path}: expected the engine-change `git diff ... HEAD -- <paths>` and found none"]
    paths = match.group(1).split()
    missing = [m for m in published if m not in paths]
    line = text.count("\n", 0, match.start()) + 1
    return [f"{path}:{line}: the engine-change filter does not watch {', '.join(missing)}"] if missing else []


def check_release_steps(path, text, published):
    """release.yml publishes each published module, one `Publish <name>` step each, in dependency order."""
    actual = re.findall(r"^\s*- name: Publish ([a-z0-9-]+)\s*$", text, re.M)
    if actual != published:
        return [f"{path}:{line_of(text, '- name: Publish')}: publish steps are out of step with modules.json ({diff_note(published, actual)})"]
    return []


def check_build_order(path, text, everything):
    # a module may be started on more than one branch of the script (a sequential and a parallel path)
    actual = list(dict.fromkeys(re.findall(r"^\s*start_module ([a-z0-9-]+)\s", text, re.M)))
    if sorted(actual) != sorted(everything):
        return [f"{path}:{line_of(text, 'start_module')}: build order is out of step with modules.json ({diff_note(everything, actual)})"]
    return []


def check_warm_list(path, text, everything):
    actual = re.findall(r'warm_module "([a-z0-9-]+)"', text)
    # the function definition itself is `warm_module() {`, which the quoted-name pattern does not match
    if sorted(actual) != sorted(everything):
        line = line_of(text, 'warm_module "')
        return [f"{path}:{line}: warm-up list is out of step with modules.json ({diff_note(everything, actual)})"]
    return []


def check_matrix(path, text, everything):
    match = re.search(r"module:\s*\[([^\]]+)\]", text)
    if not match:
        return [f"{path}: expected a `module: [...]` matrix and found none"]
    actual = [m.strip() for m in match.group(1).split(",")]
    if sorted(actual) != sorted(everything):
        line = text.count("\n", 0, match.start()) + 1
        return [f"{path}:{line}: dependency-graph matrix is out of step with modules.json ({diff_note(everything, actual)})"]
    return []


def check_mutation_jobs(path, text, mutated):
    found = []
    summary = re.search(r"^  summary:.*?^    needs: \[([^\]]*)\]", text, re.M | re.S)
    needs = [n.strip() for n in summary.group(1).split(",")] if summary else []
    for module in mutated:
        job = f"mutation-testing-{module}"
        if not re.search(rf"^  {re.escape(job)}(-incremental)?:\s*$", text, re.M):
            found.append(f"{path}: no `{job}` job, but modules.json says {module} is mutation-tested")
        elif not any(n.startswith(job) for n in needs):
            found.append(f"{path}: the `summary` job does not wait for `{job}`")
    return found


def check_adapter(root, module):
    """An adapter has a capability declaration, runs the conformance kit, and names a test for each attested claim."""
    found = []
    name = module["name"]
    declaration = module.get("capabilities")
    resources = os.path.join(root, name, "src", "main", "resources")
    if not declaration or not os.path.exists(os.path.join(resources, declaration)):
        found.append(f"{name}: no capability declaration (src/main/resources/{declaration or 'invaract-capabilities-<engine>.yaml'})")
    tests = ""
    test_dir = os.path.join(root, name, "src", "test")
    for base, _, files in os.walk(test_dir):
        for f in files:
            if f.endswith(".scala"):
                with open(os.path.join(base, f)) as fh:
                    tests += fh.read()
    for trait in ("AdapterConformanceSpec", "AttestedClaimsSpec"):
        if not re.search(rf"extends\s+{trait}\b", tests):
            found.append(f"{name}: no test extends {trait} (see docs/ADDING_AN_ENGINE_ADAPTER.md)")
    return found


def module_version(root, name):
    match = re.search(r'ThisBuild / version := "([^"]+)"', read(root, f"{name}/build.sbt"))
    return match.group(1) if match else None


def check_sibling_pins(root, modules):
    """A module's dependency on another module of this repository is that module's current version.

    A pin left behind fails only when someone next builds the module, which for a module CI does not build
    can be months later (registry-client pinned a contract version that no longer existed). The comparison
    baselines of the API-compatibility check are deliberately older and are written differently, so they are
    not matched here.
    """
    versions = {m["name"]: module_version(root, m["name"]) for m in modules}
    found = []
    for module in modules:
        sbt = read(root, f"{module['name']}/build.sbt")
        for match in re.finditer(r'"com\.invaract" %% "invaract-([a-z-]+)" % "([0-9][^"]*)"', sbt):
            dependency, pinned = match.group(1), match.group(2)
            line = sbt.count("\n", 0, match.start()) + 1
            before = sbt[max(0, match.start() - 160): match.start()]
            if "mimaPreviousArtifacts" in before:
                continue
            if dependency in versions and versions[dependency] and pinned != versions[dependency]:
                found.append(f"{module['name']}/build.sbt:{line}: depends on {dependency} {pinned}, but {dependency} is {versions[dependency]}")
    return found


def check_adapter_uses_kit(root, adapters):
    return [
        f"{m['name']}/build.sbt: no test dependency on invaract-adapter-testkit"
        for m in adapters
        if not re.search(r'"invaract-adapter-testkit" % "[^"]+" % "test"', read(root, f"{m['name']}/build.sbt"))
    ]


def check_unregistered(root, modules):
    registered = {m["name"] for m in modules}
    skip = {"node_modules", "target", ".git"}
    found = []
    for entry in sorted(os.listdir(root)):
        if entry in skip or not os.path.isdir(os.path.join(root, entry)):
            continue
        if os.path.exists(os.path.join(root, entry, "build.sbt")) and entry not in registered:
            found.append(f"{entry}/: has a build.sbt but is not in modules.json (add it there, then run this check for the rest)")
    for name in sorted(registered):
        if not os.path.exists(os.path.join(root, name, "build.sbt")):
            found.append(f"modules.json: {name} is registered but {name}/build.sbt does not exist")
    return found


def check_all(root):
    modules = load_registry(root)
    everything = names(modules)
    published = names(modules, published=True)
    mutated = names(modules, mutation=True)
    adapters = [m for m in modules if m["role"] == "adapter"]
    test_yml = read(root, ".github/workflows/test.yml")

    found = check_unregistered(root, modules)
    found += check_for_loops(".github/workflows/test.yml", test_yml, published)
    found += check_for_loops(".github/workflows/release-dry-run.yml", read(root, ".github/workflows/release-dry-run.yml"), published)
    found += check_changes_filter(".github/workflows/test.yml", test_yml, published)
    found += check_release_steps(".github/workflows/release.yml", read(root, ".github/workflows/release.yml"), published)
    built = [m["name"] for m in modules if m.get("built", True)]
    found += check_build_order("dev/build", read(root, "dev/build"), built)
    found += check_warm_list(".claude/hooks/session-start.sh", read(root, ".claude/hooks/session-start.sh"), built)
    found += check_matrix(".github/workflows/dependency-graph.yml", read(root, ".github/workflows/dependency-graph.yml"), everything)
    found += check_mutation_jobs(".github/workflows/test.yml", test_yml, mutated)
    for adapter in adapters:
        found += check_adapter(root, adapter)
    found += check_adapter_uses_kit(root, adapters)
    found += check_sibling_pins(root, modules)
    return found


def main(argv):
    root = argv[1] if len(argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    findings = check_all(os.path.abspath(root))
    for f in findings:
        print(f)
    if findings:
        print(f"\n{len(findings)} place(s) disagree with modules.json.", file=sys.stderr)
        return 1
    print("modules.json agrees with every file that lists modules.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
