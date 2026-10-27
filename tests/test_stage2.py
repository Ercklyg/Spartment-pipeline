from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from spartment.common.io import write_json
from spartment.stage2_transform.contract import INPUT_COLUMNS, SOURCE_TABLES
from spartment.stage2_transform.models import TransformInputError, TransformSchemaError, TransformSettings
from spartment.stage2_transform.pipeline import transform
from spartment.stage2_transform.writer import content_checksum

RUN_DATE = "2026-10-18"
PARENT = "spartment-extract-2026-10-18-0100"
REFERENCE = date(2026, 10, 18)

PROFILE_ID = "11111111-1111-4111-8111-111111111111"
BUILDING_ID = "22222222-2222-4222-8222-222222222222"
ROOM1_ID = "33333333-3333-4333-8333-333333333333"
ROOM2_ID = "44444444-4444-4444-8444-444444444444"
LEASE1_ID = "55555555-5555-4555-8555-555555555555"
LEASE2_ID = "66666666-6666-4666-8666-666666666666"
BILL1_ID = "77777777-7777-4777-8777-777777777777"
BILL2_ID = "88888888-8888-4888-8888-888888888888"
PAYMENT1_ID = "99999999-9999-4999-8999-999999999999"
CORRECTION1_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
INQUIRY_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
REQUEST_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
EVENT_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
NOTIFICATION_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
ACTOR_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"


def base_rows() -> dict[str, list[dict[str, str]]]:
    return {
        "profiles": [
            {
                "id": PROFILE_ID,
                "full_name": " Juan Dela Cruz ",
                "phone": "09171234567",
                "role": "tenant",
                "created_at": "2026-10-01T09:00:00+08:00",
            }
        ],
        "buildings": [
            {
                "id": BUILDING_ID,
                "name": " Spartment One ",
                "address": " 123 Main St ",
                "created_at": "2026-09-01T08:00:00+08:00",
            }
        ],
        "rooms": [
            {
                "id": ROOM1_ID,
                "building_id": BUILDING_ID,
                "room_number": " 101 ",
                "floor": "1",
                "status": "Occupied",
                "size_sqm": "12.50",
                "capacity": "2",
                "monthly_rate": "8000.00",
                "created_at": "2026-09-01T08:00:00+08:00",
                "updated_at": "2026-09-02T08:00:00+08:00",
                "description": " Corner unit ",
            },
            {
                "id": ROOM2_ID,
                "building_id": BUILDING_ID,
                "room_number": "102",
                "floor": "1",
                "status": "available",
                "size_sqm": "6.50",
                "capacity": "2",
                "monthly_rate": "7000.00",
                "created_at": "2026-09-01T08:00:00+08:00",
                "updated_at": "2026-09-02T08:00:00+08:00",
                "description": "",
            },
        ],
        "leases": [
            {
                "id": LEASE1_ID,
                "tenant_id": PROFILE_ID,
                "room_id": ROOM1_ID,
                "move_in_date": "2026-01-01",
                "lease_end_date": "2026-12-31",
                "status": "active",
            },
            {
                "id": LEASE2_ID,
                "tenant_id": PROFILE_ID,
                "room_id": ROOM2_ID,
                "move_in_date": "2025-01-01",
                "lease_end_date": "2025-06-30",
                "status": "ended",
            },
        ],
        "tenant_assignments": [
            {
                "id": PROFILE_ID,
                "room_id": ROOM1_ID,
                "full_name": "Juan Dela Cruz",
                "email": "Juan.DelaCruz@Mail.COM",
                "phone": "09171234567",
                "monthly_rent": "8000.00",
                "move_in": "2026-01-01",
                "lease_end": "2026-12-31",
                "status": "active",
                "created_at": "2026-01-01T09:00:00+08:00",
                "emergency_name": "Maria",
                "emergency_phone": "09179876543",
                "relationship": "Sister",
            }
        ],
        "bills": [
            {
                "id": BILL1_ID,
                "lease_id": LEASE1_ID,
                "bill_type": "Rent",
                "billing_period": "2026-09-01",
                "due_date": "2026-09-15",
                "rent_amount": "8000.00",
                "electricity_amount": "500.00",
                "water_amount": "200.00",
                "status": "pending",
                "created_at": "2026-09-01T08:00:00+08:00",
            },
            {
                "id": BILL2_ID,
                "lease_id": LEASE1_ID,
                "bill_type": "utility",
                "billing_period": "2026-10-01",
                "due_date": "2026-10-15",
                "rent_amount": "8000.00",
                "electricity_amount": "0.00",
                "water_amount": "0.00",
                "status": "pending",
                "created_at": "2026-10-01T08:00:00+08:00",
            },
        ],
        "payments": [
            {
                "id": PAYMENT1_ID,
                "bill_id": BILL1_ID,
                "amount": "8700.00",
                "payment_date": "2026-09-10",
                "reference_note": "Cash",
                "created_at": "2026-09-10T10:00:00+08:00",
            }
        ],
        "payment_corrections": [
            {
                "id": CORRECTION1_ID,
                "payment_id": PAYMENT1_ID,
                "corrected_by": ACTOR_ID,
                "reason": " Correct amount ",
                "previous_values": '{"amount": 8000}',
                "corrected_values": '{"amount": 8700}',
                "created_at": "2026-09-11T10:00:00+08:00",
            }
        ],
        "inquiries": [
            {
                "id": INQUIRY_ID,
                "full_name": " Ana ",
                "email": "ANA@EXAMPLE.COM",
                "phone": "09170000000",
                "preferred_room": "101",
                "move_in_date": "2026-11-01",
                "status": "New",
                "created_at": "2026-10-01T09:00:00+08:00",
                "room_id": ROOM1_ID,
                "message": "Interested",
            }
        ],
        "tenant_requests": [
            {
                "id": REQUEST_ID,
                "tenant_id": PROFILE_ID,
                "room_id": ROOM1_ID,
                "kind": "maintenance",
                "subject": " Leaky faucet ",
                "priority": "High",
                "status": "In progress",
                "created_at": "2026-10-02T09:00:00+08:00",
                "updated_at": "2026-10-03T09:00:00+08:00",
                "message": "Please fix",
                "admin_note": "Scheduled",
            }
        ],
        "tenant_occupancy_events": [
            {
                "id": EVENT_ID,
                "tenant_id": PROFILE_ID,
                "previous_lease_id": LEASE1_ID,
                "new_lease_id": LEASE2_ID,
                "performed_by": PROFILE_ID,
                "created_at": "2026-10-04T09:00:00+08:00",
                "details": '{"reason": "room change"}',
            }
        ],
        "notifications": [
            {
                "id": NOTIFICATION_ID,
                "tenant_id": PROFILE_ID,
                "title": " Bill ready ",
                "message": "Secret message",
                "kind": "bill",
                "source_id": BILL1_ID,
                "is_read": "false",
                "created_at": "2026-10-05T09:00:00+08:00",
            }
        ],
    }


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), restval="", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_batch(
    base: Path,
    rows: dict[str, list[dict[str, str]]],
    parent: str = PARENT,
    decision: str = "APPROVED",
) -> None:
    raw = base / "rawdata"
    for table in SOURCE_TABLES:
        write_csv(
            raw / table / RUN_DATE / f"{table}.csv",
            INPUT_COLUMNS[table],
            rows[table],
        )
    manifest = {
        table: {column: "text" for column in INPUT_COLUMNS[table]}
        for table in SOURCE_TABLES
    }
    handover = {
        "pipeline_run_id": parent,
        "acceptance_decision": {
            "decision": decision,
            "decided_at": "2026-10-18T01:10:00+08:00",
            "promote": decision == "APPROVED",
        },
        "processing_period": RUN_DATE,
        "raw_datasets": {
            table: f"{table}/{RUN_DATE}/{table}.csv" for table in SOURCE_TABLES
        },
        "data_contract": {"column_manifest": manifest},
    }
    write_json(raw / "_handover" / f"{parent}.json", handover)
    metadata = {
        "pipeline_run_id": parent,
        "tables": [
            {
                "table_name": table,
                "records_extracted": len(rows[table]),
                "extraction_started_at": f"{RUN_DATE}T01:00:00+08:00",
                "extraction_ended_at": f"{RUN_DATE}T01:05:00+08:00",
            }
            for table in SOURCE_TABLES
        ],
    }
    write_json(raw / "_metadata" / f"{parent}.json", metadata)
    write_json(
        raw / "_validation" / f"{parent}.json",
        {"pipeline_run_id": parent, "validation_result": "PASS"},
    )


def settings(base: Path, allow_manual_review: bool = False, dry_run: bool = False) -> TransformSettings:
    return TransformSettings(
        raw_root=base / "rawdata",
        output_root=base / "transformed",
        log_root=base / "logs" / "spartment-transformation",
        quarantine_root=base / "quarantine" / "transformation",
        reference_date=REFERENCE,
        allow_manual_review=allow_manual_review,
        dry_run=dry_run,
    )


class Stage2Tests(unittest.TestCase):
    def test_full_pipeline_approved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            result = transform(settings(base), "spartment-transform-2026-10-18-0200", PARENT)

            self.assertEqual(result.decision, "APPROVED")
            self.assertTrue(result.released)
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(len(result.output_paths), 13)
            for table, path in result.output_paths.items():
                self.assertTrue(path.is_file(), table)
            self.assertTrue(result.handover_path and result.handover_path.is_file())
            self.assertTrue(result.validation_path and result.validation_path.is_file())

            tenant = result.outputs["dim_tenant"][0]
            self.assertEqual(tenant["identity_source"], "BOTH")
            self.assertEqual(tenant["role"], "TENANT")
            self.assertEqual(tenant["full_name"], "Juan Dela Cruz")
            self.assertEqual(tenant["email"], "juan.delacruz@mail.com")
            self.assertEqual(tenant["outstanding_bill_count"], 1)
            self.assertEqual(tenant["is_flagged_tenant"], "FALSE")
            self.assertEqual(tenant["active_flag"], "TRUE")

            building = result.outputs["dim_building"][0]
            self.assertEqual(building["room_count"], 2)
            self.assertEqual(building["occupied_rooms"], 1)
            self.assertEqual(building["available_rooms"], 1)
            self.assertEqual(building["occupancy_rate"], "0.5000")
            self.assertEqual(building["vacancy_gap"], "7000.00")
            self.assertEqual(building["name"], "Spartment One")

            rooms = {row["room_id"]: row for row in result.outputs["dim_room"]}
            self.assertEqual(rooms[ROOM1_ID]["status"], "OCCUPIED")
            self.assertEqual(rooms[ROOM1_ID]["code_unmapped"], "FALSE")
            self.assertEqual(rooms[ROOM2_ID]["size_out_of_bounds"], "TRUE")

            leases = {row["lease_id"]: row for row in result.outputs["dim_lease"]}
            self.assertEqual(leases[LEASE1_ID]["is_active"], "TRUE")
            self.assertEqual(leases[LEASE1_ID]["is_open_ended"], "FALSE")
            self.assertEqual(leases[LEASE2_ID]["is_active"], "FALSE")

            bills = {row["bill_id"]: row for row in result.outputs["fact_billing"]}
            self.assertEqual(bills[BILL1_ID]["status"], "PAID")
            self.assertEqual(bills[BILL1_ID]["amount_paid"], "8700.00")
            self.assertEqual(bills[BILL1_ID]["balance"], "0.00")
            self.assertEqual(bills[BILL1_ID]["collection_rate"], "1.0000")
            self.assertEqual(bills[BILL1_ID]["is_revenue_recognized"], "TRUE")
            self.assertEqual(bills[BILL1_ID]["status_recomputed"], "TRUE")
            self.assertEqual(bills[BILL2_ID]["status"], "PENDING")
            self.assertEqual(bills[BILL2_ID]["balance"], "8000.00")

            payment = result.outputs["fact_payment"][0]
            self.assertEqual(payment["payment_allocation_rank"], 1)
            self.assertEqual(payment["correction_count"], 1)
            self.assertEqual(payment["is_corrected"], "TRUE")

            correction = result.outputs["fact_payment_correction"][0]
            self.assertEqual(correction["json_valid"], "TRUE")
            self.assertEqual(correction["previous_values"], '{"amount":8000}')

            inquiry = result.outputs["fact_inquiry"][0]
            self.assertEqual(inquiry["email"], "ana@example.com")
            self.assertEqual(inquiry["age_days"], 17)
            self.assertEqual(inquiry["is_open"], "TRUE")

            request = result.outputs["fact_tenant_request"][0]
            self.assertEqual(request["status"], "IN_PROGRESS")
            self.assertIsNone(request["resolution_days"])
            self.assertEqual(request["is_open"], "TRUE")

            event = result.outputs["fact_occupancy_event"][0]
            self.assertEqual(event["event_type"], "TRANSFER")
            self.assertIsNotNone(event["new_lease_key"])

            notification = result.outputs["fact_notification"][0]
            self.assertEqual(notification["kind"], "BILL")
            self.assertEqual(notification["is_read"], "FALSE")
            self.assertEqual(notification["title"], "Bill ready")

    def test_manual_review_is_held_without_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["rooms"][1]["status"] = "loft"
            write_batch(base, rows)
            held = transform(settings(base), "spartment-transform-2026-10-18-0300", PARENT)
            self.assertEqual(held.decision, "MANUAL REVIEW")
            self.assertFalse(held.released)
            self.assertEqual(held.exit_code, 5)

            released = transform(
                settings(base, allow_manual_review=True),
                "spartment-transform-2026-10-18-0301",
                PARENT,
            )
            self.assertEqual(released.decision, "MANUAL REVIEW")
            self.assertTrue(released.released)
            self.assertEqual(released.exit_code, 0)

    def test_quarantine_and_rule_application(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["payments"].extend(
                [
                    {
                        "id": "10000000-0000-4000-8000-000000000001",
                        "bill_id": BILL1_ID,
                        "amount": "-10.00",
                        "payment_date": "2026-09-10",
                        "reference_note": "bad",
                        "created_at": "2026-09-10T11:00:00+08:00",
                    },
                    {
                        "id": "10000000-0000-4000-8000-000000000002",
                        "bill_id": BILL1_ID,
                        "amount": "8700.0",
                        "payment_date": "2026-09-10",
                        "reference_note": "duplicate",
                        "created_at": "2026-09-10T12:00:00+08:00",
                    },
                    {
                        "id": "10000000-0000-4000-8000-000000000003",
                        "bill_id": BILL2_ID,
                        "amount": "50.00",
                        "payment_date": "2026-12-01",
                        "reference_note": "future",
                        "created_at": "2026-10-01T12:00:00+08:00",
                    },
                    {
                        "id": "10000000-0000-4000-8000-000000000004",
                        "bill_id": "20000000-0000-4000-8000-000000000004",
                        "amount": "10.00",
                        "payment_date": "2026-09-10",
                        "reference_note": "orphan",
                        "created_at": "2026-09-10T12:00:00+08:00",
                    },
                ]
            )
            rows["rooms"].append(
                {
                    "id": "20000000-0000-4000-8000-000000000001",
                    "building_id": "20000000-0000-4000-8000-000000000002",
                    "room_number": "103",
                    "floor": "1",
                    "status": "available",
                    "size_sqm": "10.00",
                    "capacity": "2",
                    "monthly_rate": "5000.00",
                    "created_at": "2026-09-01T08:00:00+08:00",
                    "updated_at": "2026-09-02T08:00:00+08:00",
                    "description": "",
                }
            )
            rows["bills"].append(
                {
                    "id": "20000000-0000-4000-8000-000000000003",
                    "lease_id": LEASE1_ID,
                    "bill_type": "rent",
                    "billing_period": "2026-10-01",
                    "due_date": "2026-09-20",
                    "rent_amount": "0.00",
                    "electricity_amount": "0.00",
                    "water_amount": "0.00",
                    "status": "pending",
                    "created_at": "2026-10-01T08:00:00+08:00",
                }
            )
            rows["leases"][0]["lease_end_date"] = "2025-12-31"
            rows["tenant_assignments"].append(
                {
                    "id": "20000000-0000-4000-8000-000000000005",
                    "room_id": ROOM2_ID,
                    "full_name": "Ended Tenant",
                    "email": "ended@example.com",
                    "phone": "09170000001",
                    "monthly_rent": "7000.00",
                    "move_in": "2025-01-01",
                    "lease_end": "2025-06-30",
                    "status": "ended",
                    "created_at": "2025-01-01T09:00:00+08:00",
                }
            )
            rows["payment_corrections"].append(
                {
                    "id": "20000000-0000-4000-8000-000000000006",
                    "payment_id": PAYMENT1_ID,
                    "corrected_by": "",
                    "reason": "bad json",
                    "previous_values": "not json",
                    "corrected_values": "",
                    "created_at": "2026-09-12T10:00:00+08:00",
                }
            )
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-0400", PARENT)

            self.assertEqual(result.decision, "REJECTED")
            self.assertEqual(result.exit_code, 1)
            self.assertTrue(result.quarantine_paths)

            rules: set[str] = set()
            for path in result.quarantine_paths.values():
                with path.open("r", encoding="utf-8", newline="") as handle:
                    for record in csv.DictReader(handle):
                        rules.add(record["rule_id"])
            self.assertIn("C-05", rules)
            self.assertIn("C-06", rules)
            self.assertIn("C-09", rules)
            self.assertIn("C-10", rules)
            self.assertIn("X-04", rules)

            corrected_bill = next(
                row
                for row in result.outputs["fact_billing"]
                if row["bill_id"] == "20000000-0000-4000-8000-000000000003"
            )
            self.assertEqual(corrected_bill["due_date"], "2026-10-16")
            self.assertEqual(corrected_bill["due_date_corrected"], "TRUE")

            corrected_lease = next(
                row for row in result.outputs["dim_lease"] if row["lease_id"] == LEASE1_ID
            )
            self.assertIsNone(corrected_lease["lease_end_date"])
            self.assertEqual(corrected_lease["date_corrected"], "TRUE")

            invalid_correction = next(
                row
                for row in result.outputs["fact_payment_correction"]
                if row["correction_id"] == "20000000-0000-4000-8000-000000000006"
            )
            self.assertEqual(invalid_correction["json_valid"], "FALSE")
            self.assertEqual(invalid_correction["previous_values"], "not json")

    def test_idempotency_content_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            first = transform(settings(base), "spartment-transform-2026-10-18-0500", PARENT)
            second = transform(settings(base), "spartment-transform-2026-10-18-0501", PARENT)
            self.assertEqual(first.decision, "APPROVED")
            self.assertEqual(second.decision, "APPROVED")
            for table in first.outputs:
                self.assertEqual(
                    content_checksum(table, first.outputs[table]),
                    content_checksum(table, second.outputs[table]),
                    table,
                )
            self.assertEqual(first.output_paths.keys(), second.output_paths.keys())

    def test_parent_rejected_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows(), decision="REJECTED")
            with self.assertRaises(TransformInputError):
                transform(settings(base), "spartment-transform-2026-10-18-0600", PARENT)

    def test_missing_required_column_is_schema_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            write_batch(base, rows)
            path = base / "rawdata" / "rooms" / RUN_DATE / "rooms.csv"
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                fieldnames = [name for name in reader.fieldnames or [] if name != "status"]
                data = list(reader)
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(data)
            with self.assertRaises(TransformSchemaError):
                transform(settings(base), "spartment-transform-2026-10-18-0700", PARENT)

    def test_manifest_missing_required_column_is_schema_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            path = base / "rawdata" / "_handover" / f"{PARENT}.json"
            handover = json.loads(path.read_text(encoding="utf-8"))
            del handover["data_contract"]["column_manifest"]["rooms"]["status"]
            write_json(path, handover)
            with self.assertRaises(TransformSchemaError):
                transform(settings(base), "spartment-transform-2026-10-18-0800", PARENT)

    def test_missing_parent_metadata_is_input_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            (base / "rawdata" / "_metadata" / f"{PARENT}.json").unlink()
            with self.assertRaises(TransformInputError):
                transform(settings(base), "spartment-transform-2026-10-18-0900", PARENT)

    def test_nullable_notification_source_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["notifications"][0]["source_id"] = ""
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1000", PARENT)
            self.assertEqual(result.decision, "APPROVED")
            self.assertIsNone(result.outputs["fact_notification"][0]["source_id"])

    def test_c13_referenced_dimension_fails_v06(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["rooms"][1]["monthly_rate"] = ""
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1100", PARENT)

            self.assertEqual(result.decision, "REJECTED")
            self.assertEqual(result.exit_code, 1)
            checks = {check.check_name: check for check in result.checks}
            self.assertEqual(checks["V-06 Referential integrity"].result, "FAIL")
            self.assertEqual(checks["V-01 Record reconciliation"].result, "PASS")
            self.assertTrue(
                any(
                    item.get("source_table") == "leases"
                    for item in checks["V-06 Referential integrity"].evidence
                )
            )

            quarantine_path = result.quarantine_paths["dim_room"]
            with quarantine_path.open("r", encoding="utf-8", newline="") as handle:
                rejected = list(csv.DictReader(handle))
            self.assertTrue(any(record["rule_id"] == "C-13" for record in rejected))

    def test_dim_date_covers_rejected_source_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["notifications"].append(
                {
                    "id": "30000000-0000-4000-8000-000000000001",
                    "tenant_id": "30000000-0000-4000-8000-000000000002",
                    "title": "Old rejected notification",
                    "message": "orphan",
                    "kind": "bill",
                    "source_id": "",
                    "is_read": "false",
                    "created_at": "2024-12-31T23:59:59+08:00",
                }
            )
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1200", PARENT)

            self.assertEqual(result.outputs["dim_date"][0]["date"], "2024-12-31")
            self.assertIn(
                "2024-12-31",
                {row["date"] for row in result.outputs["dim_date"]},
            )

    def test_rule_logs_are_attributed_to_output_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            run_id = "spartment-transform-2026-10-18-1300"
            result = transform(settings(base), run_id, PARENT)
            self.assertEqual(result.decision, "APPROVED")

            log_dir = base / "logs" / "spartment-transformation" / RUN_DATE
            with (log_dir / f"{run_id}_dim_tenant.json").open(
                "r", encoding="utf-8"
            ) as handle:
                tenant_log = json.load(handle)
            with (log_dir / f"{run_id}_fact_assignment.json").open(
                "r", encoding="utf-8"
            ) as handle:
                assignment_log = json.load(handle)

            self.assertIn("B-03", tenant_log["rules_applied"])
            self.assertNotIn("B-06", tenant_log["rules_applied"])
            self.assertIn("B-06", assignment_log["rules_applied"])
            self.assertNotIn("B-03", assignment_log["rules_applied"])

    def test_manifest_accepts_postgrest_type_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            path = base / "rawdata" / "_handover" / f"{PARENT}.json"
            handover = json.loads(path.read_text(encoding="utf-8"))
            manifest = handover["data_contract"]["column_manifest"]
            manifest["profiles"]["id"] = "uuid"
            manifest["profiles"]["created_at"] = "timestamptz"
            manifest["profiles"]["role"] = "public.app_role"
            manifest["rooms"]["status"] = "public.room_status"
            manifest["rooms"]["size_sqm"] = "numeric"
            manifest["payments"]["amount"] = "numeric"
            manifest["payment_corrections"]["previous_values"] = "jsonb"
            write_json(path, handover)
            result = transform(settings(base), "spartment-transform-2026-10-18-1359", PARENT)
            self.assertEqual(result.decision, "APPROVED")

    def test_manifest_unsupported_data_type_is_schema_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows())
            path = base / "rawdata" / "_handover" / f"{PARENT}.json"
            handover = json.loads(path.read_text(encoding="utf-8"))
            handover["data_contract"]["column_manifest"]["rooms"]["status"] = (
                "geography"
            )
            write_json(path, handover)
            with self.assertRaises(TransformSchemaError):
                transform(settings(base), "spartment-transform-2026-10-18-1400", PARENT)

    def test_null_required_reference_is_c13_not_v06(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["rooms"][0]["building_id"] = ""
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1500", PARENT)

            checks = {check.check_name: check for check in result.checks}
            self.assertEqual(checks["V-06 Referential integrity"].result, "PASS")
            quarantine_path = result.quarantine_paths["dim_room"]
            with quarantine_path.open("r", encoding="utf-8", newline="") as handle:
                rejected = list(csv.DictReader(handle))
            self.assertTrue(
                any(
                    record["rule_id"] == "C-13"
                    and "building_id" in record["rejection_reason"]
                    for record in rejected
                )
            )

    def test_payment_duplicate_is_excluded_from_v20_and_names_survivor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            duplicate_id = "10000000-0000-4000-8000-000000000009"
            rows["payments"].append(
                {
                    "id": duplicate_id,
                    "bill_id": BILL1_ID,
                    "amount": "8700.0",
                    "payment_date": "2026-09-10",
                    "reference_note": "duplicate",
                    "created_at": "2026-09-10T12:00:00+08:00",
                }
            )
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1600", PARENT)

            self.assertEqual(result.decision, "APPROVED")
            self.assertEqual(len(result.outputs["fact_payment"]), 1)
            quarantine_path = result.quarantine_paths["fact_payment"]
            with quarantine_path.open("r", encoding="utf-8", newline="") as handle:
                rejected = list(csv.DictReader(handle))
            self.assertEqual(len(rejected), 1)
            self.assertEqual(rejected[0]["rule_id"], "C-06")
            self.assertIn(PAYMENT1_ID, rejected[0]["rejection_reason"])

            checks = {check.check_name: check for check in result.checks}
            v20 = checks["V-20 Rejection rate"]
            overall = next(
                item for item in v20.evidence if item.get("table") is None
            )
            self.assertEqual(v20.result, "PASS")
            self.assertEqual(overall["rejected"], 0)
            handover = json.loads(result.handover_path.read_text(encoding="utf-8"))
            self.assertEqual(handover["rejection_rate"]["rejected"], 0)
            self.assertEqual(handover["rejection_rate"]["duplicates_discarded"], 1)
            self.assertEqual(handover["rejection_rate"]["rate"], 0.0)

    def test_assignment_duplicate_keeps_latest_created_at(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["tenant_assignments"].append(
                {
                    "id": PROFILE_ID,
                    "room_id": "",
                    "full_name": "Old Name",
                    "email": "old@example.com",
                    "phone": "09170000009",
                    "monthly_rent": "1.00",
                    "move_in": "2025-01-01",
                    "lease_end": "2025-06-30",
                    "status": "ended",
                    "created_at": "2025-01-01T09:00:00+08:00",
                }
            )
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1700", PARENT)

            self.assertEqual(result.decision, "APPROVED")
            self.assertEqual(len(result.outputs["fact_assignment"]), 1)
            assignment = result.outputs["fact_assignment"][0]
            room = next(
                row for row in result.outputs["dim_room"] if row["room_id"] == ROOM1_ID
            )
            self.assertEqual(assignment["room_key"], room["room_key"])
            with result.quarantine_paths["fact_assignment"].open(
                "r", encoding="utf-8", newline=""
            ) as handle:
                rejected = list(csv.DictReader(handle))
            self.assertEqual(rejected[0]["rule_id"], "C-06")
            self.assertNotIn("09170000009", json.dumps(rejected[0]))

    def test_parent_manual_review_requires_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            write_batch(base, base_rows(), decision="MANUAL REVIEW")
            with self.assertRaises(TransformInputError):
                transform(settings(base), "spartment-transform-2026-10-18-1800", PARENT)

            accepted = transform(
                settings(base, allow_manual_review=True),
                "spartment-transform-2026-10-18-1801",
                PARENT,
            )
            self.assertEqual(accepted.decision, "APPROVED")
            self.assertTrue(accepted.released)

    def test_v13_v14_reconcile_rejected_source_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            rows = base_rows()
            rows["payments"].append(
                {
                    "id": "10000000-0000-4000-8000-000000000010",
                    "bill_id": BILL2_ID,
                    "amount": "-5.00",
                    "payment_date": "2026-09-10",
                    "reference_note": "negative",
                    "created_at": "2026-09-10T12:00:00+08:00",
                }
            )
            rows["bills"].append(
                {
                    "id": "10000000-0000-4000-8000-000000000011",
                    "lease_id": LEASE1_ID,
                    "bill_type": "rent",
                    "billing_period": "2026-10-01",
                    "due_date": "2026-10-15",
                    "rent_amount": "-1.00",
                    "electricity_amount": "0.00",
                    "water_amount": "0.00",
                    "status": "pending",
                    "created_at": "2026-10-01T08:00:00+08:00",
                }
            )
            write_batch(base, rows)
            result = transform(settings(base), "spartment-transform-2026-10-18-1900", PARENT)

            checks = {check.check_name: check for check in result.checks}
            self.assertEqual(checks["V-13 Payment reconciliation"].result, "PASS")
            self.assertEqual(checks["V-14 Bill component reconciliation"].result, "PASS")


if __name__ == "__main__":
    unittest.main()
