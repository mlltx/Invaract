# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""Builds and checks the `invaract` package's wheels (docs/RELEASING.md, "The Python packages"; release-python.yml).

    python_wheel.py version                       # the release version, after checking every place that states it agrees
    python_wheel.py native --checker BIN --out D  # a platform wheel carrying the native checker
    python_wheel.py java --jar JAR --out D        # the pure-Python wheel, carrying the checker jar it runs with Java
    python_wheel.py smoke --wheel W --expect native|java [--extra pandas]
                                                  # installs W in a new virtual environment and runs the example job

A platform wheel's tag is read from the checker itself, so it can never claim more than the program runs on: the CPU from
its header, and on Linux the newest glibc symbol version it needs (manylinux_<x>_<y>), on macOS its minimum OS version
(macosx_<major>_0, the form pip matches from macOS 11 on). The package's version must be the checker's (pythonio-adapter's
build.sbt) and invaract-lint's, since one tag releases all three.
"""
import argparse
import hashlib
import base64
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import venv
import zipfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
ADAPTER = os.path.join(ROOT, "pythonio-adapter")
PACKAGE = os.path.join(ADAPTER, "python")
NATIVE_NAME = "invaract-pythonio-checker"
JAR_NAME = "invaract-pythonio-checker.jar"

_ELF_MACHINES = {62: "x86_64", 183: "aarch64"}
_MACHO_CPUS = {0x01000007: "x86_64", 0x0100000C: "arm64"}
_LC_VERSION_MIN_MACOSX = 0x24
_LC_BUILD_VERSION = 0x32


# ---- versions --------------------------------------------------------------------------------------------------------

def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _one(pattern, text, what):
    found = re.search(pattern, text, re.MULTILINE)
    if not found:
        raise SystemExit(f"cannot find {what}")
    return found.group(1)


def versions(root=ROOT):
    """The version each place states: the package, the checker, the lint."""
    return {
        "invaract (pythonio-adapter/python/invaract/__init__.py)":
            _one(r'^__version__ = "([^"]+)"', _read(os.path.join(root, "pythonio-adapter", "python", "invaract", "__init__.py")), "the package's version"),
        "the checker (pythonio-adapter/build.sbt)":
            _one(r'^ThisBuild / version := "([^"]+)"', _read(os.path.join(root, "pythonio-adapter", "build.sbt")), "the checker's version"),
        "invaract-lint (pythonio-adapter/lint/invaract_lint/__init__.py)":
            _one(r'^__version__ = "([^"]+)"', _read(os.path.join(root, "pythonio-adapter", "lint", "invaract_lint", "__init__.py")), "the lint's version"),
    }


def release_version(root=ROOT):
    """The one version all three state; exits naming each when they differ."""
    found = versions(root)
    if len(set(found.values())) != 1:
        raise SystemExit("the versions differ, and one release tag publishes all three:\n" +
                         "\n".join(f"  {where}: {v}" for where, v in found.items()))
    return next(iter(found.values()))


# ---- platform tags ---------------------------------------------------------------------------------------------------

def linux_tag(data):
    """``manylinux_<x>_<y>_<cpu>`` for an ELF program: the newest ``GLIBC_x.y`` it names. A string that is not a symbol
    version could only raise the tag, which narrows where pip installs the wheel; it never claims too much."""
    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise ValueError("not a 64-bit little-endian ELF program")
    machine = struct.unpack_from("<H", data, 18)[0]
    if machine not in _ELF_MACHINES:
        raise ValueError(f"an ELF program for machine {machine}, which no wheel is built for")
    needed = {(int(a), int(b)) for a, b in re.findall(rb"GLIBC_(\d+)\.(\d+)", data)}
    if not needed:
        raise ValueError("an ELF program that names no glibc version: a statically linked checker needs its own tag")
    major, minor = max(needed)
    return f"manylinux_{major}_{minor}_{_ELF_MACHINES[machine]}"


def macos_tag(data):
    """``macosx_<major>_<minor>_<cpu>`` for a Mach-O program, from its minimum OS version (``LC_BUILD_VERSION``, or the
    older ``LC_VERSION_MIN_MACOSX``). From macOS 11 on pip matches only ``<major>_0``, so the minor is dropped there."""
    magic, cpu, _, _, ncmds = struct.unpack_from("<IiiII", data, 0)
    if magic != 0xFEEDFACF:
        raise ValueError("not a 64-bit Mach-O program")
    if cpu not in _MACHO_CPUS:
        raise ValueError(f"a Mach-O program for CPU {cpu:#x}, which no wheel is built for")
    offset, minos = 32, None
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, offset)
        if cmd == _LC_BUILD_VERSION:
            minos = struct.unpack_from("<I", data, offset + 12)[0]
        elif cmd == _LC_VERSION_MIN_MACOSX:
            minos = struct.unpack_from("<I", data, offset + 8)[0]
        offset += size
    if minos is None:
        raise ValueError("a Mach-O program that states no minimum macOS version")
    major, minor = minos >> 16, (minos >> 8) & 0xFF
    if major >= 11:
        minor = 0
    return f"macosx_{major}_{minor}_{_MACHO_CPUS[cpu]}"


def platform_tag(path):
    with open(path, "rb") as f:
        data = f.read()
    return macos_tag(data) if data[:4] == b"\xcf\xfa\xed\xfe" else linux_tag(data)


# ---- building --------------------------------------------------------------------------------------------------------

def build(out, carry, name, mode):
    """Builds the package's wheel with ``carry`` inside it at ``invaract/<name>``, into ``out``; returns its path."""
    os.makedirs(out, exist_ok=True)
    with tempfile.TemporaryDirectory() as work:
        source = os.path.join(work, "source")
        shutil.copytree(PACKAGE, source, ignore=shutil.ignore_patterns(
            "tests", "examples", "build", "*.egg-info", "__pycache__", ".pytest_cache", "bin", "lib"))
        target = os.path.join(source, "invaract", name)
        os.makedirs(os.path.dirname(target))
        shutil.copyfile(carry, target)
        os.chmod(target, mode)
        dist = os.path.join(work, "dist")
        subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", dist, source], check=True)
        [built] = os.listdir(dist)
        return shutil.move(os.path.join(dist, built), os.path.join(out, built))


def retag(wheel, platform):
    """The pure wheel ``wheel`` as a platform wheel for ``platform``: its ``WHEEL`` file's tag and its file name changed,
    every entry (and its permissions) kept. Returns the new path; the old file is removed."""
    folder, name = os.path.split(wheel)
    distribution, version, _python, _abi, _platform = name[:-len(".whl")].split("-")
    tag = f"py3-none-{platform}"
    renamed = os.path.join(folder, f"{distribution}-{version}-{tag}.whl")
    info = f"{distribution}-{version}.dist-info/"
    with zipfile.ZipFile(wheel) as old, zipfile.ZipFile(renamed, "w", zipfile.ZIP_DEFLATED) as new:
        record, wheel_file = None, None
        for entry in old.infolist():
            data = old.read(entry)
            if entry.filename == info + "WHEEL":
                text = data.decode("utf-8")
                text = re.sub(r"^Root-Is-Purelib: .*$", "Root-Is-Purelib: false", text, flags=re.MULTILINE)
                text = re.sub(r"^Tag: .*\n?", "", text, flags=re.MULTILINE).rstrip("\n") + f"\nTag: {tag}\n"
                data, wheel_file = text.encode("utf-8"), (entry, text.encode("utf-8"))
            if entry.filename == info + "RECORD":
                record = entry
                continue
            new.writestr(entry, data)
        digest = base64.urlsafe_b64encode(hashlib.sha256(wheel_file[1]).digest()).rstrip(b"=").decode()
        lines = [line for line in old.read(record).decode("utf-8").splitlines() if line]
        lines = [f"{info}WHEEL,sha256={digest},{len(wheel_file[1])}" if line.startswith(info + "WHEEL,") else line
                 for line in lines]
        new.writestr(record, "\n".join(lines) + "\n")
    os.remove(wheel)
    return renamed


# ---- smoke test ------------------------------------------------------------------------------------------------------

_SMOKE = """
import os, sys
import invaract._checker as c
command = c.command(os.environ)
print("checker:", " ".join(command))
expected = {"native": [c.BUNDLED], "java": [c.BUNDLED_JAR]}[sys.argv[1]]
sys.exit(0 if command[-1:] == expected else "the wheel's own checker is not the one the package runs")
"""


def smoke(wheel, expect, extras):
    """Installs ``wheel`` (with ``extras``) in a new virtual environment and, with no ``INVARACT_CHECKER``, runs the example
    job: it must run the checker the wheel carries (``expect``: ``native`` or ``java``), publish both outputs, and refuse a
    broken report."""
    with tempfile.TemporaryDirectory() as work:
        env_dir = os.path.join(work, "venv")
        venv.create(env_dir, with_pip=True)
        python = os.path.join(env_dir, "Scripts" if os.name == "nt" else "bin", "python")
        subprocess.run([python, "-m", "pip", "install", "--quiet", wheel, *extras], check=True)
        environ = {k: v for k, v in os.environ.items() if not k.startswith("INVARACT_")}
        subprocess.run([python, "-c", _SMOKE, expect], check=True, env=environ, cwd=work)
        job = os.path.join(work, "orders_job")
        shutil.copytree(os.path.join(PACKAGE, "examples", "orders_job"), job)
        subprocess.run([python, "main.py"], check=True, env=environ, cwd=job)
        for output in ("report", "audit"):
            if not os.listdir(os.path.join(job, "data", "out", output)):
                raise SystemExit(f"the example job published nothing to data/out/{output}")
        shutil.rmtree(os.path.join(job, "data", "out"))
        contract = os.path.join(job, "contract.yaml")
        with open(contract, encoding="utf-8") as f:
            text = f.read()
        broken = re.sub(r"(name: total,\s+type: )long", r"\1string", text)
        if broken == text:
            raise SystemExit("the smoke test's contract change no longer applies to examples/orders_job/contract.yaml")
        with open(contract, "w", encoding="utf-8") as f:
            f.write(broken)
        refused = subprocess.run([python, "main.py"], env=environ, cwd=job)
        if refused.returncode != 1 or os.path.exists(os.path.join(job, "data", "out", "report")):
            raise SystemExit(f"a report breaking the contract was not refused (exit status {refused.returncode})")
    print(f"{os.path.basename(wheel)}: installs, finds its checker, publishes a passing run and refuses a failing one")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("version")
    native = commands.add_parser("native")
    native.add_argument("--checker", required=True)
    native.add_argument("--out", required=True)
    java = commands.add_parser("java")
    java.add_argument("--jar", required=True)
    java.add_argument("--out", required=True)
    check = commands.add_parser("smoke")
    check.add_argument("--wheel", required=True)
    check.add_argument("--expect", required=True, choices=("native", "java"))
    check.add_argument("--extra", action="append", default=[])
    args = parser.parse_args(argv)

    if args.command == "version":
        print(release_version())
    elif args.command == "native":
        release_version()
        tag = platform_tag(args.checker)
        print(retag(build(args.out, args.checker, os.path.join("bin", NATIVE_NAME), 0o755), tag))
    elif args.command == "java":
        release_version()
        print(build(args.out, args.jar, os.path.join("lib", JAR_NAME), 0o644))
    else:
        smoke(os.path.abspath(args.wheel), args.expect, args.extra)


if __name__ == "__main__":
    main()
