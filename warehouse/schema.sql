-- Generated from spartment.stage3_load.schema; owner-only setup uses the CLI.
-- Schema registry values are added by setup after catalog verification.
CREATE SCHEMA warehouse;

CREATE SCHEMA load_control;

CREATE TABLE load_control.schema_version (version text PRIMARY KEY, contract_hash text NOT NULL, catalog_hash text NOT NULL);

CREATE TABLE load_control.load_attempt (
          run_id text PRIMARY KEY, batch_id text, transformation_run_id text,
          status text NOT NULL CHECK (status IN ('RUNNING','SUCCESS','HELD','FAILED',
            'COMMIT_UNKNOWN','SKIPPED_ALREADY_LOADED','INTERRUPTED','DRY_RUN')),
          started_at timestamptz NOT NULL, ended_at timestamptz, detail jsonb NOT NULL);

CREATE TABLE load_control.load_table_result (
          run_id text REFERENCES load_control.load_attempt(run_id), table_name text NOT NULL,
          detail jsonb NOT NULL, PRIMARY KEY(run_id,table_name));

CREATE TABLE load_control.loaded_batch (
          batch_id text PRIMARY KEY, transformation_run_id text UNIQUE NOT NULL,
          run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
          processing_period date NOT NULL, reference_date date NOT NULL,
          published_at timestamptz NOT NULL, checksums jsonb NOT NULL, counts jsonb NOT NULL);

CREATE TABLE load_control.current_snapshot (
          singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
          batch_id text NOT NULL REFERENCES load_control.loaded_batch(batch_id));

CREATE TABLE warehouse."dim_date" (
  "date_key" character varying(8) NOT NULL,
  "date" date NOT NULL,
  "year" integer NOT NULL,
  "quarter" integer NOT NULL,
  "month" integer NOT NULL,
  "month_name" text NOT NULL,
  "week" integer NOT NULL,
  "day_of_month" integer NOT NULL,
  "day_of_week" integer NOT NULL,
  "day_name" text NOT NULL,
  "is_weekend" boolean NOT NULL,
  "billing_period" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("date_key"),
  UNIQUE ("date"),
  CHECK (date_key = to_char(date, 'YYYYMMDD')),
  CHECK (year = extract(year from date)),
  CHECK (quarter = extract(quarter from date)),
  CHECK (month = extract(month from date)),
  CHECK (week = extract(week from date)),
  CHECK (day_of_month = extract(day from date)),
  CHECK (day_of_week = extract(isodow from date))
);

CREATE INDEX "dim_date_load_batch_id_idx" ON warehouse."dim_date" ("load_batch_id");

CREATE TABLE warehouse."dim_building" (
  "building_key" character varying(16) NOT NULL,
  "building_id" uuid NOT NULL,
  "name" text NOT NULL,
  "address" text,
  "room_count" integer NOT NULL,
  "occupied_rooms" integer NOT NULL,
  "available_rooms" integer NOT NULL,
  "occupancy_rate" numeric,
  "vacancy_gap" numeric(18,2) NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("building_key"),
  UNIQUE ("building_id"),
  CHECK ("vacancy_gap" >= 0),
  CHECK ("building_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "dim_building_load_batch_id_idx" ON warehouse."dim_building" ("load_batch_id");

CREATE TABLE warehouse."dim_room" (
  "room_key" character varying(16) NOT NULL,
  "room_id" uuid NOT NULL,
  "building_key" character varying(16) NOT NULL,
  "building_id" uuid NOT NULL,
  "room_number" text NOT NULL,
  "floor" integer,
  "status" text NOT NULL,
  "status_raw" text,
  "code_unmapped" boolean NOT NULL,
  "size_sqm" numeric,
  "size_out_of_bounds" boolean NOT NULL,
  "capacity" text,
  "monthly_rate" numeric(18,2) NOT NULL,
  "description" text,
  "is_available" boolean NOT NULL,
  "is_occupied" boolean NOT NULL,
  "is_under_maintenance" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "updated_at" timestamp with time zone,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("room_key"),
  UNIQUE ("room_id"),
  FOREIGN KEY ("building_key") REFERENCES warehouse."dim_building" ("building_key"),
  CHECK ("monthly_rate" >= 0),
  CHECK ("status" IN ('AVAILABLE','MAINTENANCE','OCCUPIED','RESERVED','UNKNOWN')),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("building_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "dim_room_building_key_idx" ON warehouse."dim_room" ("building_key");

CREATE INDEX "dim_room_load_batch_id_idx" ON warehouse."dim_room" ("load_batch_id");

CREATE TABLE warehouse."dim_tenant" (
  "tenant_key" character varying(16) NOT NULL,
  "tenant_id" uuid NOT NULL,
  "full_name" text NOT NULL,
  "phone" text,
  "role" text NOT NULL,
  "is_tenant" boolean NOT NULL,
  "is_staff" boolean NOT NULL,
  "email" text,
  "current_assignment_id" uuid,
  "monthly_rent" numeric(18,2),
  "move_in" date,
  "lease_end" date,
  "assignment_status" text,
  "emergency_name" text,
  "emergency_phone" text,
  "relationship" text,
  "identity_source" text NOT NULL,
  "active_flag" boolean NOT NULL,
  "outstanding_bill_count" integer NOT NULL,
  "is_flagged_tenant" boolean NOT NULL,
  "code_unmapped" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("tenant_key"),
  UNIQUE ("tenant_id"),
  CHECK ("role" IN ('ADMIN','TENANT','UNKNOWN')),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "dim_tenant_load_batch_id_idx" ON warehouse."dim_tenant" ("load_batch_id");

CREATE TABLE warehouse."dim_lease" (
  "lease_key" character varying(16) NOT NULL,
  "lease_id" uuid NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "tenant_id" uuid NOT NULL,
  "room_key" character varying(16) NOT NULL,
  "room_id" uuid NOT NULL,
  "move_in_date" date NOT NULL,
  "lease_end_date" date,
  "status" text NOT NULL,
  "is_active" boolean NOT NULL,
  "is_open_ended" boolean NOT NULL,
  "lease_days" integer,
  "code_unmapped" boolean NOT NULL,
  "date_corrected" boolean NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("lease_key"),
  UNIQUE ("lease_id"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  FOREIGN KEY ("room_key") REFERENCES warehouse."dim_room" ("room_key"),
  CHECK ("status" IN ('ACTIVE','ENDED','UNKNOWN')),
  CHECK ("lease_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "dim_lease_load_batch_id_idx" ON warehouse."dim_lease" ("load_batch_id");

CREATE INDEX "dim_lease_room_key_idx" ON warehouse."dim_lease" ("room_key");

CREATE INDEX "dim_lease_tenant_key_idx" ON warehouse."dim_lease" ("tenant_key");

CREATE TABLE warehouse."fact_assignment" (
  "assignment_key" character varying(16) NOT NULL,
  "assignment_id" uuid NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "room_key" character varying(16),
  "monthly_rent" numeric(18,2) NOT NULL,
  "move_in" date NOT NULL,
  "lease_end" date,
  "status" text NOT NULL,
  "is_active" boolean NOT NULL,
  "tenure_days" integer,
  "code_unmapped" boolean NOT NULL,
  "date_corrected" boolean NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("assignment_key"),
  UNIQUE ("assignment_id"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  FOREIGN KEY ("room_key") REFERENCES warehouse."dim_room" ("room_key"),
  CHECK ("monthly_rent" >= 0),
  CHECK ("status" IN ('ACTIVE','ENDED','UNKNOWN')),
  CHECK ("assignment_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_assignment_load_batch_id_idx" ON warehouse."fact_assignment" ("load_batch_id");

CREATE INDEX "fact_assignment_room_key_idx" ON warehouse."fact_assignment" ("room_key");

CREATE INDEX "fact_assignment_tenant_key_idx" ON warehouse."fact_assignment" ("tenant_key");

CREATE TABLE warehouse."fact_billing" (
  "bill_key" character varying(16) NOT NULL,
  "bill_id" uuid NOT NULL,
  "lease_key" character varying(16) NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "room_key" character varying(16) NOT NULL,
  "bill_type" text NOT NULL,
  "billing_period_date" date NOT NULL,
  "billing_year" integer NOT NULL,
  "billing_month" integer NOT NULL,
  "billing_period" text NOT NULL,
  "due_date" date NOT NULL,
  "rent_amount" numeric(18,2) NOT NULL,
  "electricity_amount" numeric(18,2) NOT NULL,
  "water_amount" numeric(18,2) NOT NULL,
  "total_amount" numeric(18,2) NOT NULL,
  "amount_paid" numeric(18,2) NOT NULL,
  "balance" numeric(18,2) NOT NULL,
  "status" text NOT NULL,
  "status_recomputed" boolean NOT NULL,
  "code_unmapped" boolean NOT NULL,
  "is_rent" boolean NOT NULL,
  "is_utility" boolean NOT NULL,
  "days_to_due" integer NOT NULL,
  "days_overdue" integer NOT NULL,
  "is_overdue" boolean NOT NULL,
  "overdue_bucket" text NOT NULL,
  "is_revenue_recognized" boolean NOT NULL,
  "due_date_corrected" boolean NOT NULL,
  "collection_rate" numeric,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("bill_key"),
  UNIQUE ("bill_id"),
  FOREIGN KEY ("lease_key") REFERENCES warehouse."dim_lease" ("lease_key"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  FOREIGN KEY ("room_key") REFERENCES warehouse."dim_room" ("room_key"),
  CHECK ("rent_amount" >= 0),
  CHECK ("electricity_amount" >= 0),
  CHECK ("water_amount" >= 0),
  CHECK ("total_amount" >= 0),
  CHECK ("amount_paid" >= 0),
  CHECK ("balance" >= 0),
  CHECK ("bill_type" IN ('RENT','UNKNOWN','UTILITY')),
  CHECK ("status" IN ('PAID','PARTIAL','PENDING','UNKNOWN')),
  CHECK ("overdue_bucket" IN ('CURRENT','D1_30','D31_60','D61_90','D90_PLUS')),
  CHECK ("bill_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("lease_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_billing_lease_key_idx" ON warehouse."fact_billing" ("lease_key");

CREATE INDEX "fact_billing_load_batch_id_idx" ON warehouse."fact_billing" ("load_batch_id");

CREATE INDEX "fact_billing_room_key_idx" ON warehouse."fact_billing" ("room_key");

CREATE INDEX "fact_billing_tenant_key_idx" ON warehouse."fact_billing" ("tenant_key");

CREATE TABLE warehouse."fact_payment" (
  "payment_key" character varying(16) NOT NULL,
  "payment_id" uuid NOT NULL,
  "bill_key" character varying(16) NOT NULL,
  "lease_key" character varying(16) NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "amount" numeric(18,2) NOT NULL,
  "payment_date" date NOT NULL,
  "payment_year" integer NOT NULL,
  "payment_month" integer NOT NULL,
  "reference_note" text,
  "payment_allocation_rank" integer NOT NULL,
  "is_oldest_payment" boolean NOT NULL,
  "correction_count" integer NOT NULL,
  "is_corrected" boolean NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("payment_key"),
  UNIQUE ("payment_id"),
  FOREIGN KEY ("bill_key") REFERENCES warehouse."fact_billing" ("bill_key"),
  FOREIGN KEY ("lease_key") REFERENCES warehouse."dim_lease" ("lease_key"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  CHECK ("amount" >= 0),
  CHECK ("payment_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("bill_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("lease_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_payment_bill_key_idx" ON warehouse."fact_payment" ("bill_key");

CREATE INDEX "fact_payment_lease_key_idx" ON warehouse."fact_payment" ("lease_key");

CREATE INDEX "fact_payment_load_batch_id_idx" ON warehouse."fact_payment" ("load_batch_id");

CREATE INDEX "fact_payment_tenant_key_idx" ON warehouse."fact_payment" ("tenant_key");

CREATE TABLE warehouse."fact_payment_correction" (
  "correction_key" character varying(16) NOT NULL,
  "correction_id" uuid NOT NULL,
  "payment_key" character varying(16) NOT NULL,
  "payment_id" uuid NOT NULL,
  "corrected_by" uuid,
  "reason" text,
  "previous_values" text,
  "corrected_values" text,
  "json_valid" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("correction_key"),
  UNIQUE ("correction_id"),
  FOREIGN KEY ("payment_key") REFERENCES warehouse."fact_payment" ("payment_key"),
  CHECK ("correction_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("payment_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_payment_correction_load_batch_id_idx" ON warehouse."fact_payment_correction" ("load_batch_id");

CREATE INDEX "fact_payment_correction_payment_key_idx" ON warehouse."fact_payment_correction" ("payment_key");

CREATE TABLE warehouse."fact_inquiry" (
  "inquiry_key" character varying(16) NOT NULL,
  "inquiry_id" uuid NOT NULL,
  "room_key" character varying(16),
  "full_name" text NOT NULL,
  "email" text NOT NULL,
  "phone" text,
  "preferred_room" text,
  "move_in_date" date,
  "status" text NOT NULL,
  "code_unmapped" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "age_days" integer NOT NULL,
  "is_open" boolean NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("inquiry_key"),
  UNIQUE ("inquiry_id"),
  FOREIGN KEY ("room_key") REFERENCES warehouse."dim_room" ("room_key"),
  CHECK ("status" IN ('CONTACTED','NEW','RESOLVED','UNKNOWN')),
  CHECK ("inquiry_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_inquiry_load_batch_id_idx" ON warehouse."fact_inquiry" ("load_batch_id");

CREATE INDEX "fact_inquiry_room_key_idx" ON warehouse."fact_inquiry" ("room_key");

CREATE TABLE warehouse."fact_tenant_request" (
  "request_key" character varying(16) NOT NULL,
  "request_id" uuid NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "room_key" character varying(16),
  "kind" text NOT NULL,
  "subject" text NOT NULL,
  "priority" text NOT NULL,
  "status" text NOT NULL,
  "code_unmapped" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "updated_at" timestamp with time zone NOT NULL,
  "resolution_days" integer,
  "is_open" boolean NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("request_key"),
  UNIQUE ("request_id"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  FOREIGN KEY ("room_key") REFERENCES warehouse."dim_room" ("room_key"),
  CHECK ("kind" IN ('MAINTENANCE','OTHER','UNKNOWN')),
  CHECK ("priority" IN ('HIGH','NORMAL','UNKNOWN','URGENT')),
  CHECK ("status" IN ('IN_PROGRESS','NEW','RESOLVED','SCHEDULED','UNKNOWN')),
  CHECK ("request_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("room_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_tenant_request_load_batch_id_idx" ON warehouse."fact_tenant_request" ("load_batch_id");

CREATE INDEX "fact_tenant_request_room_key_idx" ON warehouse."fact_tenant_request" ("room_key");

CREATE INDEX "fact_tenant_request_tenant_key_idx" ON warehouse."fact_tenant_request" ("tenant_key");

CREATE TABLE warehouse."fact_occupancy_event" (
  "event_key" character varying(16) NOT NULL,
  "event_id" uuid NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "previous_lease_key" character varying(16) NOT NULL,
  "new_lease_key" character varying(16),
  "performed_by_tenant_key" character varying(16) NOT NULL,
  "event_type" text NOT NULL,
  "details" text,
  "json_valid" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("event_key"),
  UNIQUE ("event_id"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  FOREIGN KEY ("previous_lease_key") REFERENCES warehouse."dim_lease" ("lease_key"),
  FOREIGN KEY ("new_lease_key") REFERENCES warehouse."dim_lease" ("lease_key"),
  FOREIGN KEY ("performed_by_tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  CHECK ("event_type" IN ('MOVE_OUT','TRANSFER','UNKNOWN')),
  CHECK ("event_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("previous_lease_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("new_lease_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("performed_by_tenant_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_occupancy_event_load_batch_id_idx" ON warehouse."fact_occupancy_event" ("load_batch_id");

CREATE INDEX "fact_occupancy_event_new_lease_key_idx" ON warehouse."fact_occupancy_event" ("new_lease_key");

CREATE INDEX "fact_occupancy_event_performed_by_tenant_key_idx" ON warehouse."fact_occupancy_event" ("performed_by_tenant_key");

CREATE INDEX "fact_occupancy_event_previous_lease_key_idx" ON warehouse."fact_occupancy_event" ("previous_lease_key");

CREATE INDEX "fact_occupancy_event_tenant_key_idx" ON warehouse."fact_occupancy_event" ("tenant_key");

CREATE TABLE warehouse."fact_notification" (
  "notification_key" character varying(16) NOT NULL,
  "notification_id" uuid NOT NULL,
  "tenant_key" character varying(16) NOT NULL,
  "kind" text NOT NULL,
  "code_unmapped" boolean NOT NULL,
  "title" text NOT NULL,
  "source_id" uuid,
  "is_read" boolean NOT NULL,
  "created_at" timestamp with time zone NOT NULL,
  "transformation_run_id" text NOT NULL,
  load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
  load_batch_id text NOT NULL,
  loaded_at timestamptz NOT NULL,
  PRIMARY KEY ("notification_key"),
  UNIQUE ("notification_id"),
  FOREIGN KEY ("tenant_key") REFERENCES warehouse."dim_tenant" ("tenant_key"),
  CHECK ("kind" IN ('BILL','PAYMENT','REQUEST','TENANCY','UNKNOWN')),
  CHECK ("notification_key" ~ '^[0-9a-f]{16}$'),
  CHECK ("tenant_key" ~ '^[0-9a-f]{16}$')
);

CREATE INDEX "fact_notification_load_batch_id_idx" ON warehouse."fact_notification" ("load_batch_id");

CREATE INDEX "fact_notification_tenant_key_idx" ON warehouse."fact_notification" ("tenant_key");

CREATE INDEX fact_billing_reporting_idx ON warehouse.fact_billing(tenant_key,status,due_date);

CREATE INDEX fact_payment_reporting_idx ON warehouse.fact_payment(bill_key,payment_date);

CREATE VIEW warehouse.snapshot_status AS
            SELECT b.batch_id,b.run_id,b.transformation_run_id,b.processing_period,
            b.reference_date,b.published_at,b.counts FROM load_control.current_snapshot s
            JOIN load_control.loaded_batch b USING(batch_id);
