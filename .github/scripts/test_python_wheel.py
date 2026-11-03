# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Invaract Contributors
"""python_wheel.py: the release version check, the platform tag read from a checker program, and the retagging of a wheel.
(Building and smoke-testing real wheels is release-python.yml's job, on each platform.)"""
import base64
import hashlib
import os
import shutil
import struct
import tempfile
import unittest
import zipfile

import python_wheel as pw


def elf(machine=62, strings=(b"GLIBC_2.17", b"GLIBC_2.34"), bits=2, endian=1):
    header = b"\x7fELF" + bytes([bits, endian]) + bytes(12) + struct.pack("<H", machine)
    return header + bytes(64) + b"\0".join(strings) + b"\0"


def macho(cpu=0x0100000C, commands=()):
    body = b"".join(commands)
    return struct.pack("<IiiIIII", 0xFEEDFACF, cpu, 0, 2, len(commands), len(body), 0) + bytes(4) + body


def build_version(major, minor, patch=0):
    return struct.pack("<IIIIII", pw._LC_BUILD_VERSION, 24, 1, (major << 16) | (minor << 8) | patch, 0, 0)


def version_min(major, minor):
    return struct.pack("<IIII", pw._LC_VERSION_MIN_MACOSX, 16, (major << 16) | (minor << 8), 0)


def other_command():
    return struct.pack("<II", 0x19, 16) + bytes(8)


class VersionTest(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.write("pythonio-adapter/python/invaract/__init__.py", '"""invaract"""\nimport functools\n__version__ = "1.2.0"\n')
        self.write("pythonio-adapter/build.sbt", 'name := "x"\nThisBuild / version := "1.2.0"\n')
        self.write("pythonio-adapter/lint/invaract_lint/__init__.py", '"""lint"""\n__version__ = "1.2.0"\n')

    def write(self, path, text):
        full = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(text)

    def test_the_repository_states_one_version(self):
        self.assertRegex(pw.release_version(), r"^\d+\.\d+\.\d+")

    def test_agreeing_versions_are_the_release_version(self):
        self.assertEqual(pw.release_version(self.root), "1.2.0")

    def test_a_version_that_differs_stops_the_release_naming_each_place(self):
        self.write("pythonio-adapter/lint/invaract_lint/__init__.py", '__version__ = "1.3.0"\n')
        with self.assertRaises(SystemExit) as e:
            pw.release_version(self.root)
        self.assertIn("invaract-lint (pythonio-adapter/lint/invaract_lint/__init__.py): 1.3.0", str(e.exception))
        self.assertIn("the checker (pythonio-adapter/build.sbt): 1.2.0", str(e.exception))

    def test_a_missing_version_is_named(self):
        self.write("pythonio-adapter/build.sbt", 'version := "1.2.0"\n')
        with self.assertRaisesRegex(SystemExit, "the checker's version"):
            pw.versions(self.root)


class LinuxTagTest(unittest.TestCase):

    def test_the_newest_glibc_named_and_the_cpu(self):
        self.assertEqual(pw.linux_tag(elf()), "manylinux_2_34_x86_64")
        self.assertEqual(pw.linux_tag(elf(machine=183, strings=(b"GLIBC_2.17",))), "manylinux_2_17_aarch64")

    def test_versions_compare_as_numbers_not_text(self):
        self.assertEqual(pw.linux_tag(elf(strings=(b"GLIBC_2.9", b"GLIBC_2.17", b"GLIBC_2.2.5"))), "manylinux_2_17_x86_64")

    def test_a_program_naming_no_glibc_has_no_tag(self):
        with self.assertRaisesRegex(ValueError, "statically linked"):
            pw.linux_tag(elf(strings=(b"GLIBC_PRIVATE",)))

    def test_only_64_bit_little_endian_programs_for_known_cpus(self):
        for data in (elf(bits=1), elf(endian=2), b"MZ" + bytes(100)):
            with self.assertRaisesRegex(ValueError, "not a 64-bit little-endian ELF"):
                pw.linux_tag(data)
        with self.assertRaisesRegex(ValueError, "machine 40"):
            pw.linux_tag(elf(machine=40))


class MacosTagTest(unittest.TestCase):

    def test_from_macos_11_on_only_the_major_version_counts(self):
        self.assertEqual(pw.macos_tag(macho(commands=(other_command(), build_version(11, 0)))), "macosx_11_0_arm64")
        self.assertEqual(pw.macos_tag(macho(commands=(build_version(13, 5, 1),))), "macosx_13_0_arm64")

    def test_before_macos_11_the_minor_version_counts_and_the_older_command_is_read(self):
        self.assertEqual(pw.macos_tag(macho(cpu=0x01000007, commands=(version_min(10, 13), other_command()))),
                         "macosx_10_13_x86_64")
        self.assertEqual(pw.macos_tag(macho(cpu=0x01000007, commands=(build_version(10, 15),))), "macosx_10_15_x86_64")

    def test_a_program_stating_no_minimum_or_for_another_cpu_has_no_tag(self):
        with self.assertRaisesRegex(ValueError, "no minimum macOS version"):
            pw.macos_tag(macho(commands=(other_command(),)))
        with self.assertRaisesRegex(ValueError, "CPU 0x7"):
            pw.macos_tag(macho(cpu=7, commands=(build_version(11, 0),)))
        with self.assertRaisesRegex(ValueError, "not a 64-bit Mach-O"):
            pw.macos_tag(elf())

    def test_the_platform_tag_of_a_file_reads_whichever_kind_it_is(self):
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder)
        for name, data, tag in (("linux", elf(), "manylinux_2_34_x86_64"),
                                ("mac", macho(commands=(build_version(14, 2),)), "macosx_14_0_arm64")):
            path = os.path.join(folder, name)
            with open(path, "wb") as f:
                f.write(data)
            self.assertEqual(pw.platform_tag(path), tag)


class RetagTest(unittest.TestCase):

    def setUp(self):
        self.folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.folder)
        self.wheel = os.path.join(self.folder, "invaract-1.2.0-py3-none-any.whl")
        info = "invaract-1.2.0.dist-info/"
        wheel_text = "Wheel-Version: 1.0\nGenerator: setuptools\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"
        with zipfile.ZipFile(self.wheel, "w") as z:
            z.writestr("invaract/__init__.py", "x = 1\n")
            checker = zipfile.ZipInfo("invaract/bin/invaract-pythonio-checker")
            checker.external_attr = 0o100755 << 16
            z.writestr(checker, b"\x7fELF program")
            z.writestr(info + "WHEEL", wheel_text)
            z.writestr(info + "RECORD", f"invaract/__init__.py,sha256=abc,6\n{info}WHEEL,sha256=old,{len(wheel_text)}\n{info}RECORD,,\n")

    def test_the_tag_and_name_change_and_every_entry_is_kept_with_its_permissions(self):
        renamed = pw.retag(self.wheel, "manylinux_2_34_x86_64")
        self.assertEqual(os.path.basename(renamed), "invaract-1.2.0-py3-none-manylinux_2_34_x86_64.whl")
        self.assertFalse(os.path.exists(self.wheel))
        with zipfile.ZipFile(renamed) as z:
            wheel_text = z.read("invaract-1.2.0.dist-info/WHEEL").decode()
            self.assertEqual(wheel_text, "Wheel-Version: 1.0\nGenerator: setuptools\nRoot-Is-Purelib: false\n"
                                         "Tag: py3-none-manylinux_2_34_x86_64\n")
            self.assertEqual(z.read("invaract/bin/invaract-pythonio-checker"), b"\x7fELF program")
            self.assertEqual(z.getinfo("invaract/bin/invaract-pythonio-checker").external_attr >> 16, 0o100755)
            self.assertEqual(z.read("invaract/__init__.py"), b"x = 1\n")
            digest = base64.urlsafe_b64encode(hashlib.sha256(wheel_text.encode()).digest()).rstrip(b"=").decode()
            self.assertEqual(z.read("invaract-1.2.0.dist-info/RECORD").decode().splitlines(), [
                "invaract/__init__.py,sha256=abc,6",
                f"invaract-1.2.0.dist-info/WHEEL,sha256={digest},{len(wheel_text)}",
                "invaract-1.2.0.dist-info/RECORD,,",
            ])
            self.assertEqual(sorted(z.namelist()), sorted([
                "invaract/__init__.py", "invaract/bin/invaract-pythonio-checker",
                "invaract-1.2.0.dist-info/WHEEL", "invaract-1.2.0.dist-info/RECORD"]))


if __name__ == "__main__":
    unittest.main()
