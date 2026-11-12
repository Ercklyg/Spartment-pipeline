"""Regression coverage for legitimate nullable dates/references versus quality loss."""
from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from test_stage2 import base_rows, write_batch, settings, PARENT
from spartment.stage2_transform.pipeline import transform


class OptionalNullProfileTests(unittest.TestCase):
    def run_batch(self, root, rows, suffix):
        parent = PARENT + "-" + suffix
        write_batch(root, rows, parent=parent)
        return transform(settings(root), "spartment-transform-2026-10-18-" + suffix, parent)

    def test_open_ended_dates_and_unselected_inquiry_room_are_approved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = base_rows()
            for lease in rows["leases"]:
                lease["lease_end_date"] = ""
            rows["tenant_assignments"][0]["lease_end"] = ""
            rows["inquiries"][0]["room_id"] = ""
            result = self.run_batch(root, rows, "permitted")
            self.assertEqual(result.decision, "APPROVED")
            self.assertTrue(result.released)
            self.assertTrue(all(row["lease_end_date"] is None for row in result.outputs["dim_lease"]))
            self.assertIsNone(result.outputs["fact_inquiry"][0]["room_key"])
            log = json.loads((root / "logs/spartment-transformation/2026-10-18/spartment-transform-2026-10-18-permitted_dim_lease.json").read_text())
            profile = log["optional_null_profile"]["lease_end_date"]
            self.assertEqual(profile["raw_null_rate"], 1.0)
            self.assertEqual(profile["expected_nulls"], 2)
            self.assertEqual(profile["unexpected_null_rate"], 0.0)

    def test_high_missing_contact_rate_still_holds_batch(self):
        with tempfile.TemporaryDirectory() as temp:
            rows = base_rows()
            rows["inquiries"][0]["phone"] = ""
            result = self.run_batch(Path(temp), rows, "missing-contact")
            self.assertEqual(result.decision, "MANUAL REVIEW")
            self.assertFalse(result.released)
            check = next(c for c in result.checks if c.check_name.startswith("V-18"))
            self.assertEqual(check.result, "FAIL")
            self.assertTrue(any(e["column"] == "phone" and e["unexpected_nulls"] == 1 for e in check.evidence))

    def test_increased_contact_nulls_still_warn_against_same_profile_version(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = base_rows()
            second = deepcopy(rows["inquiries"][0])
            second["id"] = "abababab-abab-4bab-8bab-abababababab"
            rows["inquiries"].append(second)
            first = self.run_batch(root, rows, "baseline")
            self.assertEqual(first.decision, "APPROVED")
            rows["inquiries"][1]["phone"] = ""
            result = self.run_batch(root, rows, "degraded")
            self.assertEqual(result.decision, "MANUAL REVIEW")
            check = next(c for c in result.checks if c.check_name.startswith("V-18"))
            self.assertEqual(check.result, "WARNING")
            self.assertTrue(any(e.get("reason") == "increased" and e["unexpected_null_rate"] == .5 for e in check.evidence))

    def test_legacy_raw_baseline_is_not_compared_to_new_eligible_population(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / "logs/spartment-transformation/2026-10-18"
            folder.mkdir(parents=True)
            (folder / "old_fact_inquiry.json").write_text(json.dumps({"run_id":"old", "rules_version":"1.0", "null_rates":{"phone":0.0}}))
            rows = base_rows()
            second = deepcopy(rows["inquiries"][0])
            second.update(id="abababab-abab-4bab-8bab-abababababab", phone="")
            rows["inquiries"].append(second)
            result = self.run_batch(root, rows, "migrated")
            check = next(c for c in result.checks if c.check_name.startswith("V-18"))
            self.assertEqual(check.result, "PASS")


if __name__ == "__main__":
    unittest.main()
