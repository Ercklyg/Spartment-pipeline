# Data Contract

Machine readable source of truth: `src/spartment/stage1_extract/contract.py`. This document is the human readable companion. Types were verified against the live PostgREST schema; full column detail is in [extraction.md](extraction.md) section 2.1.

## Tables

12 tables, 98 columns.

| Table | Columns | Required identifiers | Purpose |
| --- | --- | --- | --- |
| `profiles` | 5 | `id`, `role` | Tenant and staff identity |
| `buildings` | 4 | `id`, `name` | Building identification |
| `rooms` | 11 | `id`, `status` | Availability, capacity, rates |
| `leases` | 6 | `id`, `tenant_id`, `move_in_date` | Tenancy spine |
| `tenant_assignments` | 13 | `id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `status` | Tenant to room assignment |
| `bills` | 10 | `id`, `billing_period`, `due_date` | Billing obligations |
| `payments` | 6 | `id`, `bill_id`, `amount`, `payment_date` | Cash receipts |
| `payment_corrections` | 7 | `id`, `payment_id`, `reason`, `created_at` | Payment audit trail |
| `inquiries` | 10 | `id`, `email`, `status` | Prospective tenants |
| `tenant_requests` | 11 | `id`, `tenant_id`, `kind`, `subject`, `priority`, `status` | Tenant requests |
| `tenant_occupancy_events` | 7 | `id`, `tenant_id`, `previous_lease_id`, `performed_by`, `created_at` | Occupancy transitions |
| `notifications` | 8 | `id`, `tenant_id`, `title`, `created_at` | Event audit trail |

## Relationships

17 edges. Null values are permitted and are not treated as orphans.

| Child | Column | Parent |
| --- | --- | --- |
| `rooms` | `building_id` | `buildings.id` |
| `leases` | `tenant_id` | `profiles.id` |
| `leases` | `room_id` | `rooms.id` |
| `tenant_assignments` | `room_id` | `rooms.id` |
| `tenant_assignments` | `id` | `auth.users.id` — unverifiable |
| `bills` | `lease_id` | `leases.id` |
| `payments` | `bill_id` | `bills.id` |
| `payment_corrections` | `payment_id` | `payments.id` |
| `payment_corrections` | `corrected_by` | `auth.users.id` — unverifiable |
| `inquiries` | `room_id` | `rooms.id` |
| `tenant_requests` | `tenant_id` | `profiles.id` |
| `tenant_requests` | `room_id` | `rooms.id` |
| `tenant_occupancy_events` | `tenant_id` | `profiles.id` |
| `tenant_occupancy_events` | `previous_lease_id` | `leases.id` |
| `tenant_occupancy_events` | `new_lease_id` | `leases.id` |
| `tenant_occupancy_events` | `performed_by` | `profiles.id` |
| `notifications` | `tenant_id` | `profiles.id` |

`auth.users` is outside extraction scope, so those two edges are reported as `target_table_unavailable` and excluded from the acceptance decision.

## Legitimate nulls

These nulls are correct and must not be flagged:

| Column | When null |
| --- | --- |
| `tenant_assignments.room_id` | assignment status is `ended` |
| `tenant_assignments.lease_end` | open ended tenancy |
| `tenant_requests.room_id` | request kind is `other` |
| `tenant_occupancy_events.new_lease_id` | move out |
| `inquiries.phone` | not supplied |

`inquiries.room_id` and `leases.lease_end_date` are permitted but not expected to be null.

## Optional columns

Selected only when the live schema exposes them.

| Table | Column | Note |
| --- | --- | --- |
| `inquiries` | `room_id` | Present in the deployed database, absent from every file in `migrations/` |

## Extra columns

Columns the original contract omitted, selected by default. Disable with `--no-extra-columns`.

| Table | Columns |
| --- | --- |
| `rooms` | `description` |
| `tenant_assignments` | `emergency_name`, `emergency_phone`, `relationship` |
| `inquiries` | `message` |
| `tenant_requests` | `message`, `admin_note` |
| `tenant_occupancy_events` | `details` |

`tenant_occupancy_events.details` is `NOT NULL` and holds the actual move date, room, rent and end date. Without it that table has no payload.

## Mapping

All 98 columns map `Direct`. Source names, values and types are preserved. No renaming, no type coercion, no trimming, no unit conversion. Column types are recorded in every run manifest because CSV stores values as text.

## Domain rules

| Rule | Scope |
| --- | --- |
| Observed enum domains | `profiles.role`, `rooms.status`, `leases.status`, `bills.status` |
| Monetary, must not be negative | `rooms.monthly_rate`, `tenant_assignments.monthly_rent`, `bills.rent_amount`/`electricity_amount`/`water_amount`, `payments.amount` |
| Numeric bounds | `rooms.size_sqm` between 8 and 14 |
| Date ordering | `tenant_assignments.lease_end >= move_in`, `leases.lease_end_date >= move_in_date` |
| Invariant | `tenant_assignments`: `active` requires a `room_id`, `ended` requires none |