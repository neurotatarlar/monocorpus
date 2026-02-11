"""Unit tests for pps ZIP rewrite helpers."""

import os
import sys
import tempfile
import unittest
import zipfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from content.pps.service import _write_zip_with_updated_md  # noqa: E402


class PpsZipIoTests(unittest.TestCase):
    def test_write_zip_with_updated_md_preserves_other_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zpath = os.path.join(tmp, "doc.zip")
            target_md = "abc.md"
            other_md = "notes.md"
            bin_name = "img.bin"
            txt_name = "sub/info.txt"

            original_bin = b"\x00\x01\x02binary"
            original_other_md = "other markdown"
            original_txt = "nested file"

            with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(target_md, "old content")
                zf.writestr(other_md, original_other_md)
                zf.writestr(bin_name, original_bin)
                zf.writestr(txt_name, original_txt)

            _write_zip_with_updated_md(zpath, target_md, "new content")

            with zipfile.ZipFile(zpath, "r") as zf:
                names = zf.namelist()
                self.assertEqual(1, names.count(target_md))
                self.assertEqual("new content", zf.read(target_md).decode("utf-8"))
                self.assertEqual(original_other_md, zf.read(other_md).decode("utf-8"))
                self.assertEqual(original_bin, zf.read(bin_name))
                self.assertEqual(original_txt, zf.read(txt_name).decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
