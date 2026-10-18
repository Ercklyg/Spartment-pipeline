-- Remove blanket grants, including column grants left by older schema exports.
revoke all on all tables in schema public from public,anon,authenticated;
do $$ declare item record; begin
 for item in select table_name,string_agg(quote_ident(column_name),',') columns
   from information_schema.columns where table_schema='public' group by table_name loop
   execute format('revoke select (%s),insert (%s),update (%s),references (%s) on public.%I from public,anon,authenticated',item.columns,item.columns,item.columns,item.columns,item.table_name);
 end loop;
end $$;
grant select on public.buildings,public.rooms to anon,authenticated;
grant insert on public.inquiries to anon,authenticated;
grant insert on public.buildings,public.rooms to authenticated;
grant update(status) on public.rooms,public.inquiries to authenticated;
grant select on public.profiles,public.leases,public.bills,public.payments,public.notifications,
 public.inquiries,public.tenant_assignments,public.tenant_requests,public.payment_corrections,public.tenant_occupancy_events to authenticated;
grant update(is_read) on public.notifications to authenticated;
grant update(status,admin_note) on public.tenant_requests to authenticated;

-- Trigger functions are invoked by their triggers, not exposed as callable APIs.
do $$ declare item record; begin
 for item in select p.oid::regprocedure signature from pg_proc p join pg_namespace n on n.oid=p.pronamespace
 where n.nspname='public' and p.prokind='f' and not exists
 (select 1 from pg_depend d where d.objid=p.oid and d.classid='pg_proc'::regclass and d.deptype='e') loop
   execute format('revoke execute on function %s from public,anon,authenticated',item.signature);
 end loop;
end $$;
grant execute on function public.is_admin() to anon,authenticated;
grant execute on function public.tenant_admin(),public.create_bill(uuid,text,date,date,numeric,numeric,numeric),
 public.record_bill_payment(uuid,uuid,numeric,date,text),public.correct_bill_payment(uuid,uuid,numeric,date,text,text,jsonb),
 public.submit_tenant_request(uuid,text,text,text,text),public.change_tenant_occupancy(uuid,uuid,uuid,date,uuid,numeric,date),
 public.billing_facts(date),public.billing_page(date,text,text,text,integer,integer),
 public.tenant_directory_page(integer,integer),public.analytics_report(date,text,boolean) to authenticated;
alter default privileges in schema public revoke all on tables from public,anon,authenticated;
alter default privileges in schema public revoke execute on functions from public,anon,authenticated;

create table public.login_attempt_limits (
 key_hash text primary key check(key_hash ~ '^[a-f0-9]{64}$'),
 attempts integer not null check(attempts>0),expires_at timestamptz not null
);
create index login_attempt_expiry_idx on public.login_attempt_limits(expires_at);
alter table public.login_attempt_limits enable row level security;
revoke all on public.login_attempt_limits from public,anon,authenticated;
grant all on public.login_attempt_limits to service_role;

create function public.consume_login_attempt(p_account_hash text,p_ip_hash text)
returns jsonb language plpgsql set search_path='' as $$
declare item record; entry public.login_attempt_limits; allowed boolean:=true; retry integer:=0;
begin
 if p_account_hash is null or p_ip_hash is null or p_account_hash=p_ip_hash
 or p_account_hash !~ '^[a-f0-9]{64}$' or p_ip_hash !~ '^[a-f0-9]{64}$' then raise exception 'Invalid limit keys'; end if;
 delete from public.login_attempt_limits where expires_at<=now();
 -- Stable lock order also handles simultaneous attempts from several instances.
 for item in select * from (values(p_account_hash,5),(p_ip_hash,30)) as limits(key,maximum) order by key loop
   insert into public.login_attempt_limits(key_hash,attempts,expires_at) values(item.key,1,now()+interval '15 minutes')
   on conflict(key_hash) do update set attempts=least(public.login_attempt_limits.attempts+1,item.maximum+1)
   returning * into entry;
   if entry.attempts>item.maximum then allowed:=false;retry:=greatest(retry,ceil(extract(epoch from entry.expires_at-now()))::integer); end if;
 end loop;
 return jsonb_build_object('allowed',allowed,'retryAfter',greatest(retry,1));
end $$;
revoke all on function public.consume_login_attempt(text,text) from public,anon,authenticated;
grant execute on function public.consume_login_attempt(text,text) to service_role;
