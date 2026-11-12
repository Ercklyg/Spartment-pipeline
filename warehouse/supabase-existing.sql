--
-- PostgreSQL database dump
--

\restrict XKzKnSQObfAR5FcNafWSnvX6FmZGEJFf82vifX8GFLkJzxLG4FSwZZe3T3wTqGx

-- Dumped from database version 17.11
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: warehouse; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA warehouse;


--
-- Name: warehouse_meta; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA warehouse_meta;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dim_building; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_building (
    building_key character(16) NOT NULL,
    building_id uuid NOT NULL,
    name character varying(200) NOT NULL,
    address text,
    room_count integer NOT NULL,
    occupied_rooms integer NOT NULL,
    available_rooms integer NOT NULL,
    occupancy_rate numeric(9,4),
    vacancy_gap numeric(12,2) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT dim_building_check_0 CHECK ((vacancy_gap >= (0)::numeric)),
    CONSTRAINT dim_building_check_1 CHECK ((building_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: dim_date; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_date (
    date_key character(8) NOT NULL,
    date date NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    month_name character varying(9) NOT NULL,
    week smallint NOT NULL,
    day_of_month smallint NOT NULL,
    day_of_week smallint NOT NULL,
    day_name character varying(9) NOT NULL,
    is_weekend boolean NOT NULL,
    billing_period character(7) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT dim_date_check_0 CHECK ((date_key ~ '^[0-9]{8}$'::text)),
    CONSTRAINT dim_date_check_1 CHECK (((quarter >= 1) AND (quarter <= 4))),
    CONSTRAINT dim_date_check_2 CHECK (((month >= 1) AND (month <= 12))),
    CONSTRAINT dim_date_check_3 CHECK (((day_of_week >= 1) AND (day_of_week <= 7)))
);


--
-- Name: dim_lease; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_lease (
    lease_key character(16) NOT NULL,
    lease_id uuid NOT NULL,
    tenant_key character(16) NOT NULL,
    tenant_id uuid NOT NULL,
    room_key character(16) NOT NULL,
    room_id uuid NOT NULL,
    move_in_date date NOT NULL,
    lease_end_date date,
    status character varying(20) NOT NULL,
    is_active boolean NOT NULL,
    is_open_ended boolean NOT NULL,
    lease_days integer,
    code_unmapped boolean NOT NULL,
    date_corrected boolean NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT dim_lease_check_0 CHECK (((status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'ENDED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT dim_lease_check_1 CHECK (((lease_end_date IS NULL) OR (lease_end_date >= move_in_date))),
    CONSTRAINT dim_lease_check_2 CHECK ((lease_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT dim_lease_check_3 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT dim_lease_check_4 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: dim_room; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_room (
    room_key character(16) NOT NULL,
    room_id uuid NOT NULL,
    building_key character(16) NOT NULL,
    building_id uuid NOT NULL,
    room_number character varying(50) NOT NULL,
    floor smallint,
    status character varying(20) NOT NULL,
    status_raw character varying(50),
    code_unmapped boolean NOT NULL,
    size_sqm numeric(6,2),
    size_out_of_bounds boolean NOT NULL,
    capacity character varying(50),
    monthly_rate numeric(12,2) NOT NULL,
    description text,
    is_available boolean NOT NULL,
    is_occupied boolean NOT NULL,
    is_under_maintenance boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT dim_room_check_0 CHECK ((monthly_rate >= (0)::numeric)),
    CONSTRAINT dim_room_check_1 CHECK (((status)::text = ANY ((ARRAY['AVAILABLE'::character varying, 'MAINTENANCE'::character varying, 'OCCUPIED'::character varying, 'RESERVED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT dim_room_check_2 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT dim_room_check_3 CHECK ((building_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: dim_tenant; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_tenant (
    tenant_key character(16) NOT NULL,
    tenant_id uuid NOT NULL,
    full_name character varying(200) NOT NULL,
    phone character varying(50),
    role character varying(20) NOT NULL,
    is_tenant boolean NOT NULL,
    is_staff boolean NOT NULL,
    email character varying(255),
    current_assignment_id uuid,
    monthly_rent numeric(12,2),
    move_in date,
    lease_end date,
    assignment_status character varying(20),
    emergency_name character varying(200),
    emergency_phone character varying(50),
    relationship character varying(100),
    identity_source character varying(20) NOT NULL,
    active_flag boolean NOT NULL,
    outstanding_bill_count integer NOT NULL,
    is_flagged_tenant boolean NOT NULL,
    code_unmapped boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT dim_tenant_check_0 CHECK (((role)::text = ANY ((ARRAY['ADMIN'::character varying, 'TENANT'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT dim_tenant_check_1 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_assignment; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_assignment (
    assignment_key character(16) NOT NULL,
    assignment_id uuid NOT NULL,
    tenant_key character(16) NOT NULL,
    room_key character(16),
    monthly_rent numeric(12,2) NOT NULL,
    move_in date NOT NULL,
    lease_end date,
    status character varying(20) NOT NULL,
    is_active boolean NOT NULL,
    tenure_days integer,
    code_unmapped boolean NOT NULL,
    date_corrected boolean NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_assignment_check_0 CHECK ((monthly_rent >= (0)::numeric)),
    CONSTRAINT fact_assignment_check_1 CHECK (((status)::text = ANY ((ARRAY['ACTIVE'::character varying, 'ENDED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_assignment_check_2 CHECK (((lease_end IS NULL) OR (lease_end >= move_in))),
    CONSTRAINT fact_assignment_check_3 CHECK ((((status)::text <> 'ACTIVE'::text) OR (room_key IS NOT NULL))),
    CONSTRAINT fact_assignment_check_4 CHECK ((((status)::text <> 'ENDED'::text) OR (room_key IS NULL))),
    CONSTRAINT fact_assignment_check_5 CHECK ((assignment_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_assignment_check_6 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_assignment_check_7 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_billing; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_billing (
    bill_key character(16) NOT NULL,
    bill_id uuid NOT NULL,
    lease_key character(16) NOT NULL,
    tenant_key character(16) NOT NULL,
    room_key character(16) NOT NULL,
    bill_type character varying(20) NOT NULL,
    billing_period_date date NOT NULL,
    billing_year smallint NOT NULL,
    billing_month smallint NOT NULL,
    billing_period character(7) NOT NULL,
    due_date date NOT NULL,
    rent_amount numeric(12,2) NOT NULL,
    electricity_amount numeric(12,2) NOT NULL,
    water_amount numeric(12,2) NOT NULL,
    total_amount numeric(12,2) NOT NULL,
    amount_paid numeric(12,2) NOT NULL,
    balance numeric(12,2) NOT NULL,
    status character varying(20) NOT NULL,
    status_recomputed boolean NOT NULL,
    code_unmapped boolean NOT NULL,
    is_rent boolean NOT NULL,
    is_utility boolean NOT NULL,
    days_to_due integer NOT NULL,
    days_overdue integer NOT NULL,
    is_overdue boolean NOT NULL,
    overdue_bucket character varying(10) NOT NULL,
    is_revenue_recognized boolean NOT NULL,
    due_date_corrected boolean NOT NULL,
    collection_rate numeric(9,4),
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_billing_check_0 CHECK ((rent_amount >= (0)::numeric)),
    CONSTRAINT fact_billing_check_1 CHECK ((electricity_amount >= (0)::numeric)),
    CONSTRAINT fact_billing_check_10 CHECK ((billing_period ~ '^[0-9]{4}-[0-9]{2}$'::text)),
    CONSTRAINT fact_billing_check_11 CHECK ((bill_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_billing_check_12 CHECK ((lease_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_billing_check_13 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_billing_check_14 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_billing_check_2 CHECK ((water_amount >= (0)::numeric)),
    CONSTRAINT fact_billing_check_3 CHECK ((total_amount >= (0)::numeric)),
    CONSTRAINT fact_billing_check_4 CHECK ((amount_paid >= (0)::numeric)),
    CONSTRAINT fact_billing_check_5 CHECK ((balance >= (0)::numeric)),
    CONSTRAINT fact_billing_check_6 CHECK (((bill_type)::text = ANY ((ARRAY['RENT'::character varying, 'UNKNOWN'::character varying, 'UTILITY'::character varying])::text[]))),
    CONSTRAINT fact_billing_check_7 CHECK (((status)::text = ANY ((ARRAY['PAID'::character varying, 'PARTIAL'::character varying, 'PENDING'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_billing_check_8 CHECK (((overdue_bucket)::text = ANY ((ARRAY['CURRENT'::character varying, 'D1_30'::character varying, 'D31_60'::character varying, 'D61_90'::character varying, 'D90_PLUS'::character varying])::text[]))),
    CONSTRAINT fact_billing_check_9 CHECK ((due_date >= billing_period_date))
);


--
-- Name: fact_inquiry; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_inquiry (
    inquiry_key character(16) NOT NULL,
    inquiry_id uuid NOT NULL,
    room_key character(16),
    full_name character varying(200) NOT NULL,
    email character varying(255) NOT NULL,
    phone character varying(50),
    preferred_room character varying(100),
    move_in_date date,
    status character varying(20) NOT NULL,
    code_unmapped boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    age_days integer NOT NULL,
    is_open boolean NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_inquiry_check_0 CHECK (((status)::text = ANY ((ARRAY['CONTACTED'::character varying, 'NEW'::character varying, 'RESOLVED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_inquiry_check_1 CHECK ((inquiry_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_inquiry_check_2 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_notification; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_notification (
    notification_key character(16) NOT NULL,
    notification_id uuid NOT NULL,
    tenant_key character(16) NOT NULL,
    kind character varying(30) NOT NULL,
    code_unmapped boolean NOT NULL,
    title character varying(300) NOT NULL,
    source_id uuid,
    is_read boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_notification_check_0 CHECK (((kind)::text = ANY ((ARRAY['BILL'::character varying, 'PAYMENT'::character varying, 'REQUEST'::character varying, 'TENANCY'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_notification_check_1 CHECK ((notification_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_notification_check_2 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_occupancy_event; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_occupancy_event (
    event_key character(16) NOT NULL,
    event_id uuid NOT NULL,
    tenant_key character(16) NOT NULL,
    previous_lease_key character(16) NOT NULL,
    new_lease_key character(16),
    performed_by_tenant_key character(16) NOT NULL,
    event_type character varying(20) NOT NULL,
    details text,
    json_valid boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_occupancy_event_check_0 CHECK (((event_type)::text = ANY ((ARRAY['MOVE_OUT'::character varying, 'TRANSFER'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_occupancy_event_check_1 CHECK ((event_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_occupancy_event_check_2 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_occupancy_event_check_3 CHECK ((previous_lease_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_occupancy_event_check_4 CHECK ((new_lease_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_occupancy_event_check_5 CHECK ((performed_by_tenant_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_payment; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_payment (
    payment_key character(16) NOT NULL,
    payment_id uuid NOT NULL,
    bill_key character(16) NOT NULL,
    lease_key character(16) NOT NULL,
    tenant_key character(16) NOT NULL,
    amount numeric(12,2) NOT NULL,
    payment_date date NOT NULL,
    payment_year smallint NOT NULL,
    payment_month smallint NOT NULL,
    reference_note text,
    payment_allocation_rank integer NOT NULL,
    is_oldest_payment boolean NOT NULL,
    correction_count integer NOT NULL,
    is_corrected boolean NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_payment_check_0 CHECK ((amount >= (0)::numeric)),
    CONSTRAINT fact_payment_check_1 CHECK ((payment_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_payment_check_2 CHECK ((bill_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_payment_check_3 CHECK ((lease_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_payment_check_4 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_payment_correction; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_payment_correction (
    correction_key character(16) NOT NULL,
    correction_id uuid NOT NULL,
    payment_key character(16) NOT NULL,
    payment_id uuid NOT NULL,
    corrected_by uuid,
    reason text,
    previous_values text,
    corrected_values text,
    json_valid boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_payment_correction_check_0 CHECK ((correction_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_payment_correction_check_1 CHECK ((payment_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: fact_tenant_request; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_tenant_request (
    request_key character(16) NOT NULL,
    request_id uuid NOT NULL,
    tenant_key character(16) NOT NULL,
    room_key character(16),
    kind character varying(30) NOT NULL,
    subject character varying(300) NOT NULL,
    priority character varying(20) NOT NULL,
    status character varying(20) NOT NULL,
    code_unmapped boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    resolution_days integer,
    is_open boolean NOT NULL,
    transformation_run_id character varying(64) NOT NULL,
    load_batch_id text NOT NULL,
    CONSTRAINT fact_tenant_request_check_0 CHECK (((kind)::text = ANY ((ARRAY['MAINTENANCE'::character varying, 'OTHER'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_tenant_request_check_1 CHECK (((priority)::text = ANY ((ARRAY['HIGH'::character varying, 'NORMAL'::character varying, 'UNKNOWN'::character varying, 'URGENT'::character varying])::text[]))),
    CONSTRAINT fact_tenant_request_check_2 CHECK (((status)::text = ANY ((ARRAY['IN_PROGRESS'::character varying, 'NEW'::character varying, 'RESOLVED'::character varying, 'SCHEDULED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT fact_tenant_request_check_3 CHECK ((request_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_tenant_request_check_4 CHECK ((tenant_key ~ '^[0-9a-f]{16}$'::text)),
    CONSTRAINT fact_tenant_request_check_5 CHECK ((room_key ~ '^[0-9a-f]{16}$'::text))
);


--
-- Name: attempts; Type: TABLE; Schema: warehouse_meta; Owner: -
--

CREATE TABLE warehouse_meta.attempts (
    run_id text NOT NULL,
    batch_id text,
    status text NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    report jsonb NOT NULL
);


--
-- Name: batches; Type: TABLE; Schema: warehouse_meta; Owner: -
--

CREATE TABLE warehouse_meta.batches (
    batch_id text NOT NULL,
    transformation_run_id text NOT NULL,
    extraction_run_id text NOT NULL,
    processing_period date NOT NULL,
    snapshot_end timestamp with time zone NOT NULL,
    fingerprint text NOT NULL,
    run_id text NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL,
    report jsonb NOT NULL
);


--
-- Name: current_batch; Type: TABLE; Schema: warehouse_meta; Owner: -
--

CREATE TABLE warehouse_meta.current_batch (
    singleton boolean DEFAULT true NOT NULL,
    batch_id text NOT NULL,
    CONSTRAINT current_batch_singleton_check CHECK (singleton)
);


--
-- Name: schema_contract; Type: TABLE; Schema: warehouse_meta; Owner: -
--

CREATE TABLE warehouse_meta.schema_contract (
    version text NOT NULL,
    fingerprint text NOT NULL,
    catalog jsonb NOT NULL
);


--
-- Name: dim_building dim_building_building_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_building
    ADD CONSTRAINT dim_building_building_id_key UNIQUE (building_id);


--
-- Name: dim_building dim_building_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_building
    ADD CONSTRAINT dim_building_pkey PRIMARY KEY (building_key);


--
-- Name: dim_date dim_date_date_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date
    ADD CONSTRAINT dim_date_date_key UNIQUE (date);


--
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_key);


--
-- Name: dim_lease dim_lease_lease_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_lease
    ADD CONSTRAINT dim_lease_lease_id_key UNIQUE (lease_id);


--
-- Name: dim_lease dim_lease_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_lease
    ADD CONSTRAINT dim_lease_pkey PRIMARY KEY (lease_key);


--
-- Name: dim_room dim_room_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_room
    ADD CONSTRAINT dim_room_pkey PRIMARY KEY (room_key);


--
-- Name: dim_room dim_room_room_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_room
    ADD CONSTRAINT dim_room_room_id_key UNIQUE (room_id);


--
-- Name: dim_tenant dim_tenant_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_tenant
    ADD CONSTRAINT dim_tenant_pkey PRIMARY KEY (tenant_key);


--
-- Name: dim_tenant dim_tenant_tenant_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_tenant
    ADD CONSTRAINT dim_tenant_tenant_id_key UNIQUE (tenant_id);


--
-- Name: fact_assignment fact_assignment_assignment_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_assignment
    ADD CONSTRAINT fact_assignment_assignment_id_key UNIQUE (assignment_id);


--
-- Name: fact_assignment fact_assignment_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_assignment
    ADD CONSTRAINT fact_assignment_pkey PRIMARY KEY (assignment_key);


--
-- Name: fact_billing fact_billing_bill_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_bill_id_key UNIQUE (bill_id);


--
-- Name: fact_billing fact_billing_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_pkey PRIMARY KEY (bill_key);


--
-- Name: fact_inquiry fact_inquiry_inquiry_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_inquiry
    ADD CONSTRAINT fact_inquiry_inquiry_id_key UNIQUE (inquiry_id);


--
-- Name: fact_inquiry fact_inquiry_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_inquiry
    ADD CONSTRAINT fact_inquiry_pkey PRIMARY KEY (inquiry_key);


--
-- Name: fact_notification fact_notification_notification_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_notification
    ADD CONSTRAINT fact_notification_notification_id_key UNIQUE (notification_id);


--
-- Name: fact_notification fact_notification_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_notification
    ADD CONSTRAINT fact_notification_pkey PRIMARY KEY (notification_key);


--
-- Name: fact_occupancy_event fact_occupancy_event_event_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_event_id_key UNIQUE (event_id);


--
-- Name: fact_occupancy_event fact_occupancy_event_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_pkey PRIMARY KEY (event_key);


--
-- Name: fact_payment_correction fact_payment_correction_correction_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment_correction
    ADD CONSTRAINT fact_payment_correction_correction_id_key UNIQUE (correction_id);


--
-- Name: fact_payment_correction fact_payment_correction_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment_correction
    ADD CONSTRAINT fact_payment_correction_pkey PRIMARY KEY (correction_key);


--
-- Name: fact_payment fact_payment_payment_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_payment_id_key UNIQUE (payment_id);


--
-- Name: fact_payment fact_payment_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_pkey PRIMARY KEY (payment_key);


--
-- Name: fact_tenant_request fact_tenant_request_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_tenant_request
    ADD CONSTRAINT fact_tenant_request_pkey PRIMARY KEY (request_key);


--
-- Name: fact_tenant_request fact_tenant_request_request_id_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_tenant_request
    ADD CONSTRAINT fact_tenant_request_request_id_key UNIQUE (request_id);


--
-- Name: attempts attempts_pkey; Type: CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.attempts
    ADD CONSTRAINT attempts_pkey PRIMARY KEY (run_id);


--
-- Name: batches batches_pkey; Type: CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.batches
    ADD CONSTRAINT batches_pkey PRIMARY KEY (batch_id);


--
-- Name: batches batches_transformation_run_id_key; Type: CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.batches
    ADD CONSTRAINT batches_transformation_run_id_key UNIQUE (transformation_run_id);


--
-- Name: current_batch current_batch_pkey; Type: CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.current_batch
    ADD CONSTRAINT current_batch_pkey PRIMARY KEY (singleton);


--
-- Name: schema_contract schema_contract_pkey; Type: CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.schema_contract
    ADD CONSTRAINT schema_contract_pkey PRIMARY KEY (version);


--
-- Name: dim_building_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_building_load_batch_id_idx ON warehouse.dim_building USING btree (load_batch_id);


--
-- Name: dim_date_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_date_load_batch_id_idx ON warehouse.dim_date USING btree (load_batch_id);


--
-- Name: dim_lease_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_lease_load_batch_id_idx ON warehouse.dim_lease USING btree (load_batch_id);


--
-- Name: dim_lease_room_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_lease_room_key_idx ON warehouse.dim_lease USING btree (room_key);


--
-- Name: dim_lease_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_lease_tenant_key_idx ON warehouse.dim_lease USING btree (tenant_key);


--
-- Name: dim_room_building_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_room_building_key_idx ON warehouse.dim_room USING btree (building_key);


--
-- Name: dim_room_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_room_load_batch_id_idx ON warehouse.dim_room USING btree (load_batch_id);


--
-- Name: dim_tenant_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX dim_tenant_load_batch_id_idx ON warehouse.dim_tenant USING btree (load_batch_id);


--
-- Name: fact_assignment_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_assignment_load_batch_id_idx ON warehouse.fact_assignment USING btree (load_batch_id);


--
-- Name: fact_assignment_room_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_assignment_room_key_idx ON warehouse.fact_assignment USING btree (room_key);


--
-- Name: fact_assignment_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_assignment_tenant_key_idx ON warehouse.fact_assignment USING btree (tenant_key);


--
-- Name: fact_billing_billing_period_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_billing_period_idx ON warehouse.fact_billing USING btree (billing_period);


--
-- Name: fact_billing_lease_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_lease_key_idx ON warehouse.fact_billing USING btree (lease_key);


--
-- Name: fact_billing_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_load_batch_id_idx ON warehouse.fact_billing USING btree (load_batch_id);


--
-- Name: fact_billing_room_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_room_key_idx ON warehouse.fact_billing USING btree (room_key);


--
-- Name: fact_billing_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_tenant_key_idx ON warehouse.fact_billing USING btree (tenant_key);


--
-- Name: fact_billing_tenant_key_status_due_date_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_billing_tenant_key_status_due_date_idx ON warehouse.fact_billing USING btree (tenant_key, status, due_date);


--
-- Name: fact_inquiry_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_inquiry_load_batch_id_idx ON warehouse.fact_inquiry USING btree (load_batch_id);


--
-- Name: fact_inquiry_room_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_inquiry_room_key_idx ON warehouse.fact_inquiry USING btree (room_key);


--
-- Name: fact_notification_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_notification_load_batch_id_idx ON warehouse.fact_notification USING btree (load_batch_id);


--
-- Name: fact_notification_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_notification_tenant_key_idx ON warehouse.fact_notification USING btree (tenant_key);


--
-- Name: fact_occupancy_event_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_occupancy_event_load_batch_id_idx ON warehouse.fact_occupancy_event USING btree (load_batch_id);


--
-- Name: fact_occupancy_event_new_lease_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_occupancy_event_new_lease_key_idx ON warehouse.fact_occupancy_event USING btree (new_lease_key);


--
-- Name: fact_occupancy_event_performed_by_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_occupancy_event_performed_by_tenant_key_idx ON warehouse.fact_occupancy_event USING btree (performed_by_tenant_key);


--
-- Name: fact_occupancy_event_previous_lease_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_occupancy_event_previous_lease_key_idx ON warehouse.fact_occupancy_event USING btree (previous_lease_key);


--
-- Name: fact_occupancy_event_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_occupancy_event_tenant_key_idx ON warehouse.fact_occupancy_event USING btree (tenant_key);


--
-- Name: fact_payment_bill_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_bill_key_idx ON warehouse.fact_payment USING btree (bill_key);


--
-- Name: fact_payment_bill_key_payment_date_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_bill_key_payment_date_idx ON warehouse.fact_payment USING btree (bill_key, payment_date);


--
-- Name: fact_payment_correction_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_correction_load_batch_id_idx ON warehouse.fact_payment_correction USING btree (load_batch_id);


--
-- Name: fact_payment_correction_payment_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_correction_payment_key_idx ON warehouse.fact_payment_correction USING btree (payment_key);


--
-- Name: fact_payment_lease_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_lease_key_idx ON warehouse.fact_payment USING btree (lease_key);


--
-- Name: fact_payment_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_load_batch_id_idx ON warehouse.fact_payment USING btree (load_batch_id);


--
-- Name: fact_payment_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_payment_tenant_key_idx ON warehouse.fact_payment USING btree (tenant_key);


--
-- Name: fact_tenant_request_load_batch_id_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_tenant_request_load_batch_id_idx ON warehouse.fact_tenant_request USING btree (load_batch_id);


--
-- Name: fact_tenant_request_room_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_tenant_request_room_key_idx ON warehouse.fact_tenant_request USING btree (room_key);


--
-- Name: fact_tenant_request_tenant_key_idx; Type: INDEX; Schema: warehouse; Owner: -
--

CREATE INDEX fact_tenant_request_tenant_key_idx ON warehouse.fact_tenant_request USING btree (tenant_key);


--
-- Name: dim_building dim_building_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_building
    ADD CONSTRAINT dim_building_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dim_date dim_date_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date
    ADD CONSTRAINT dim_date_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dim_lease dim_lease_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_lease
    ADD CONSTRAINT dim_lease_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dim_lease dim_lease_room_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_lease
    ADD CONSTRAINT dim_lease_room_key_fkey FOREIGN KEY (room_key) REFERENCES warehouse.dim_room(room_key);


--
-- Name: dim_lease dim_lease_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_lease
    ADD CONSTRAINT dim_lease_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: dim_room dim_room_building_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_room
    ADD CONSTRAINT dim_room_building_key_fkey FOREIGN KEY (building_key) REFERENCES warehouse.dim_building(building_key);


--
-- Name: dim_room dim_room_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_room
    ADD CONSTRAINT dim_room_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: dim_tenant dim_tenant_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_tenant
    ADD CONSTRAINT dim_tenant_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_assignment fact_assignment_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_assignment
    ADD CONSTRAINT fact_assignment_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_assignment fact_assignment_room_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_assignment
    ADD CONSTRAINT fact_assignment_room_key_fkey FOREIGN KEY (room_key) REFERENCES warehouse.dim_room(room_key);


--
-- Name: fact_assignment fact_assignment_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_assignment
    ADD CONSTRAINT fact_assignment_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_billing fact_billing_lease_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_lease_key_fkey FOREIGN KEY (lease_key) REFERENCES warehouse.dim_lease(lease_key);


--
-- Name: fact_billing fact_billing_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_billing fact_billing_room_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_room_key_fkey FOREIGN KEY (room_key) REFERENCES warehouse.dim_room(room_key);


--
-- Name: fact_billing fact_billing_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_billing
    ADD CONSTRAINT fact_billing_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_inquiry fact_inquiry_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_inquiry
    ADD CONSTRAINT fact_inquiry_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_inquiry fact_inquiry_room_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_inquiry
    ADD CONSTRAINT fact_inquiry_room_key_fkey FOREIGN KEY (room_key) REFERENCES warehouse.dim_room(room_key);


--
-- Name: fact_notification fact_notification_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_notification
    ADD CONSTRAINT fact_notification_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_notification fact_notification_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_notification
    ADD CONSTRAINT fact_notification_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_occupancy_event fact_occupancy_event_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_occupancy_event fact_occupancy_event_new_lease_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_new_lease_key_fkey FOREIGN KEY (new_lease_key) REFERENCES warehouse.dim_lease(lease_key);


--
-- Name: fact_occupancy_event fact_occupancy_event_performed_by_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_performed_by_tenant_key_fkey FOREIGN KEY (performed_by_tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_occupancy_event fact_occupancy_event_previous_lease_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_previous_lease_key_fkey FOREIGN KEY (previous_lease_key) REFERENCES warehouse.dim_lease(lease_key);


--
-- Name: fact_occupancy_event fact_occupancy_event_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_occupancy_event
    ADD CONSTRAINT fact_occupancy_event_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_payment fact_payment_bill_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_bill_key_fkey FOREIGN KEY (bill_key) REFERENCES warehouse.fact_billing(bill_key);


--
-- Name: fact_payment_correction fact_payment_correction_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment_correction
    ADD CONSTRAINT fact_payment_correction_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_payment_correction fact_payment_correction_payment_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment_correction
    ADD CONSTRAINT fact_payment_correction_payment_key_fkey FOREIGN KEY (payment_key) REFERENCES warehouse.fact_payment(payment_key);


--
-- Name: fact_payment fact_payment_lease_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_lease_key_fkey FOREIGN KEY (lease_key) REFERENCES warehouse.dim_lease(lease_key);


--
-- Name: fact_payment fact_payment_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_payment fact_payment_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_payment
    ADD CONSTRAINT fact_payment_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: fact_tenant_request fact_tenant_request_load_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_tenant_request
    ADD CONSTRAINT fact_tenant_request_load_batch_id_fkey FOREIGN KEY (load_batch_id) REFERENCES warehouse_meta.batches(batch_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: fact_tenant_request fact_tenant_request_room_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_tenant_request
    ADD CONSTRAINT fact_tenant_request_room_key_fkey FOREIGN KEY (room_key) REFERENCES warehouse.dim_room(room_key);


--
-- Name: fact_tenant_request fact_tenant_request_tenant_key_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_tenant_request
    ADD CONSTRAINT fact_tenant_request_tenant_key_fkey FOREIGN KEY (tenant_key) REFERENCES warehouse.dim_tenant(tenant_key);


--
-- Name: current_batch current_batch_batch_id_fkey; Type: FK CONSTRAINT; Schema: warehouse_meta; Owner: -
--

ALTER TABLE ONLY warehouse_meta.current_batch
    ADD CONSTRAINT current_batch_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES warehouse_meta.batches(batch_id);


--
-- PostgreSQL database dump complete
--

\unrestrict XKzKnSQObfAR5FcNafWSnvX6FmZGEJFf82vifX8GFLkJzxLG4FSwZZe3T3wTqGx
