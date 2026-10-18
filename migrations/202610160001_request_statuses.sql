BEGIN;

ALTER TABLE public.inquiries
  DROP CONSTRAINT inquiries_status_check,
  ALTER COLUMN status SET DEFAULT 'New';
UPDATE public.inquiries SET status = 'New' WHERE status = 'Open';
ALTER TABLE public.inquiries
  ADD CONSTRAINT inquiries_status_check CHECK (status IN ('New', 'Contacted', 'Resolved'));

DROP POLICY "Anyone can submit an inquiry" ON public.inquiries;
CREATE POLICY "Anyone can submit an inquiry" ON public.inquiries FOR INSERT TO anon, authenticated
WITH CHECK (status = 'New' AND length(trim(full_name)) BETWEEN 1 AND 160
  AND length(email) BETWEEN 3 AND 254 AND email ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  AND length(coalesce(phone,'')) <= 40 AND length(coalesce(message,'')) <= 4000
  AND (move_in_date IS NULL OR move_in_date >= (now() AT TIME ZONE 'Asia/Manila')::date));

ALTER TABLE public.tenant_requests
  DROP CONSTRAINT tenant_requests_status_check,
  ALTER COLUMN status SET DEFAULT 'New';
UPDATE public.tenant_requests SET status = 'New' WHERE status = 'Open';
ALTER TABLE public.tenant_requests
  ADD CONSTRAINT tenant_requests_status_check CHECK (status IN ('New', 'Scheduled', 'In progress', 'Resolved'));

COMMIT;
