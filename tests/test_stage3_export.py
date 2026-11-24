"""CSV export integrity and atomic publication without a database."""
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from spartment.stage3_load.contract import OUTPUT_COLUMNS, OUTPUT_TABLES
from spartment.stage3_load.export import export_csv
from spartment.stage3_load.models import LoadError, Package
from spartment.stage3_load.package import sha256


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        folder = self.root / "frozen"
        folder.mkdir()
        checksums = {}
        for table in OUTPUT_TABLES:
            path = folder / f"{table}.csv"
            with path.open("w", newline="", encoding="utf-8") as stream:
                csv.writer(stream).writerow(OUTPUT_COLUMNS[table])
            checksums[table] = sha256(path)
        self.package = Package({"output_checksums": checksums}, {},
                               {t: 0 for t in OUTPUT_TABLES}, folder, 0)
        self.result = dict(run_id="spartment-load-test", batch_id="batch-test",
                           transformation_run_id="transform-test", published_at="2026-11-24T12:00:00+08:00")

    def test_empty_tables_and_manifest(self):
        path = export_csv(self.root, self.package, self.result)
        manifest = json.loads((path / "manifest.json").read_text())
        self.assertEqual(manifest["batch_id"], self.result["batch_id"])
        self.assertEqual(len(list(path.glob("*.csv"))), 13)
        for table in OUTPUT_TABLES:
            entry = manifest["tables"][table]
            self.assertEqual(entry["row_count"], 0)
            self.assertEqual(entry["sha256"], sha256(path / entry["file"]))
            with (path / entry["file"]).open(newline="", encoding="utf-8") as stream:
                self.assertEqual(list(csv.reader(stream)), [list(OUTPUT_COLUMNS[table])])
        with self.assertRaises(LoadError):
            export_csv(self.root, self.package, self.result)

    def test_failed_copy_does_not_publish_partial_folder(self):
        import shutil
        copy = shutil.copyfile
        calls = []
        def fail_second(source, target):
            calls.append(source)
            if len(calls) == 2:
                raise OSError("disk full")
            return copy(source, target)
        with patch("spartment.stage3_load.export.shutil.copyfile", side_effect=fail_second):
            with self.assertRaises(OSError):
                export_csv(self.root, self.package, self.result)
        self.assertEqual(list((self.root / "loaded" / "exports").iterdir()), [])
        self.assertEqual(len(list(self.package.folder.glob("*.csv"))), 13)

    def test_changed_input_cannot_be_exported(self):
        (self.package.folder / f"{OUTPUT_TABLES[0]}.csv").write_text("changed")
        with self.assertRaises(LoadError):
            export_csv(self.root, self.package, self.result)
        self.assertEqual(list((self.root / "loaded" / "exports").iterdir()), [])
