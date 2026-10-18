-- Aggregate behind RLS; never bypass the caller's tenant visibility.
create index if not exists payments_bill_date_idx on public.payments(bill_id,payment_date);
create index if not exists payments_date_idx on public.payments(payment_date);
create index if not exists bills_period_idx on public.bills(billing_period,created_at,id);
create index if not exists notifications_tenant_created_idx on public.notifications(tenant_id,created_at desc,id);
create index if not exists assignments_status_created_idx on public.tenant_assignments(status,created_at desc,id);

create function public.billing_facts(p_cutoff date default null)
returns table(id uuid,lease_id uuid,tenant_id uuid,bill_type text,billing_period date,due_date date,
 rent_amount numeric,electricity_amount numeric,water_amount numeric,total numeric,paid numeric,balance numeric,
 status text,name text,room_number text,building_name text)
language sql stable security invoker set search_path='' as $$
 with payments as (select p.bill_id,sum(p.amount) paid from public.payments p
   where p_cutoff is null or p.payment_date<=p_cutoff group by p.bill_id),
 amounts as (select b.*,l.tenant_id,coalesce(p.paid,0) paid,
   b.rent_amount+b.electricity_amount+b.water_amount total,
   coalesce(pr.full_name,'Unknown tenant') name,r.room_number::text,coalesce(bu.name,'Building') building_name
   from public.bills b left join payments p on p.bill_id=b.id
   left join public.leases l on l.id=b.lease_id left join public.profiles pr on pr.id=l.tenant_id
   left join public.rooms r on r.id=l.room_id left join public.buildings bu on bu.id=r.building_id
   where p_cutoff is null or b.billing_period<=p_cutoff)
 select a.id,a.lease_id,a.tenant_id,a.bill_type,a.billing_period,a.due_date,a.rent_amount,a.electricity_amount,a.water_amount,
 a.total,a.paid,greatest(0,a.total-a.paid),
 case when a.paid>=a.total then 'Paid' when a.due_date<coalesce(p_cutoff,(now() at time zone 'Asia/Manila')::date) then 'Late'
 when a.paid>0 then 'Partially paid' else 'Pending' end,a.name,a.room_number,a.building_name from amounts a;
$$;

create function public.tenant_directory_page(p_offset integer default 0,p_limit integer default 25)
returns jsonb language plpgsql stable security invoker set search_path='' as $$
declare result jsonb;
begin
 if not public.is_admin() then raise exception 'Administrator access required'; end if;
 if p_offset<0 or p_limit not between 1 and 100 then raise exception 'Invalid page'; end if;
 with page as (select a.*,jsonb_build_object('room_number',r.room_number,'buildings',jsonb_build_object('name',bu.name)) rooms
   from public.tenant_assignments a left join public.rooms r on r.id=a.room_id left join public.buildings bu on bu.id=r.building_id
   where a.status='active' order by a.created_at desc,a.id offset p_offset limit p_limit),
 balances as (select f.tenant_id,count(*) count,sum(f.balance) balance,
   coalesce(sum(f.balance) filter(where f.status='Late'),0) overdue,bool_or(f.balance>0 and f.paid>0) partial
   from public.billing_facts() f where f.tenant_id in(select id from page) group by f.tenant_id)
 select jsonb_build_object('data',coalesce((select jsonb_agg(to_jsonb(p)||jsonb_build_object('billing',jsonb_build_object(
   'status',case when b.count is null then 'No bills' when b.balance=0 then 'Paid' when b.overdue>0 then 'Late' when b.partial then 'Partially paid' else 'Pending' end,
   'billCount',coalesce(b.count,0),'balance',coalesce(b.balance,0),'overdue',coalesce(b.overdue,0))) order by p.created_at desc,p.id)
   from page p left join balances b on b.tenant_id=p.id),'[]'::jsonb),
   'total',(select count(*) from public.tenant_assignments where status='active'),'offset',p_offset,'limit',p_limit) into result;
 return result;
end $$;

create function public.billing_page(p_month date,p_type text,p_status text default 'All',p_search text default '',p_offset integer default 0,p_limit integer default 25)
returns jsonb language plpgsql stable security invoker set search_path='' as $$
declare result jsonb;
begin
 if not public.is_admin() then raise exception 'Administrator access required'; end if;
 if p_month is null or p_type not in ('rent','utility') or p_status not in ('All','Paid','Pending','Partially paid','Late')
 or length(p_search)>160 or p_offset<0 or p_limit not between 1 and 100 then raise exception 'Invalid billing filters'; end if;
 with month_rows as materialized (select * from public.billing_facts() where bill_type=p_type
   and billing_period>=date_trunc('month',p_month)::date and billing_period<(date_trunc('month',p_month)+interval '1 month')::date),
 filtered as (select * from month_rows where (p_status='All' or status=p_status)
   and strpos(lower(concat_ws(' ',name,room_number,building_name)),lower(p_search))>0),
 page as (select * from filtered order by billing_period desc,due_date desc,id offset p_offset limit p_limit)
 select jsonb_build_object('bills',coalesce((select jsonb_agg(to_jsonb(p)||jsonb_build_object('displayStatus',p.status,
   'payments',coalesce((select jsonb_agg(to_jsonb(pay) order by pay.payment_date,pay.id) from public.payments pay where pay.bill_id=p.id),'[]'::jsonb),
   'leases',jsonb_build_object('tenant_id',p.tenant_id,'profiles',jsonb_build_object('full_name',p.name),'rooms',jsonb_build_object('room_number',p.room_number,'buildings',jsonb_build_object('name',p.building_name)))) order by p.billing_period desc,p.due_date desc,p.id) from page p),'[]'::jsonb),
   'total',(select count(*) from filtered),'periodTotal',(select count(*) from month_rows),'offset',p_offset,'limit',p_limit,
   'totals',(select jsonb_build_object('total',coalesce(sum(total),0),'paid',coalesce(sum(paid),0),'balance',coalesce(sum(balance),0),
   'electricity',coalesce(sum(electricity_amount),0),'water',coalesce(sum(water_amount),0),'overdue',coalesce(sum(balance) filter(where status='Late'),0)) from month_rows)) into result;
 return result;
end $$;

create function public.analytics_report(p_month date,p_period text default 'Monthly',p_export boolean default false)
returns jsonb language plpgsql stable security invoker set search_path='' as $$
declare cutoff date; step integer; anchor date; result jsonb;
begin
 if not public.is_admin() then raise exception 'Administrator access required'; end if;
 if p_month is null or p_period not in ('Monthly','Quarterly','Yearly') then raise exception 'Invalid report period'; end if;
 cutoff:=least((date_trunc('month',p_month)+interval '1 month - 1 day')::date,(now() at time zone 'Asia/Manila')::date);
 step:=case p_period when 'Yearly' then 12 when 'Quarterly' then 3 else 1 end;
 anchor:=date_trunc(case p_period when 'Yearly' then 'year' when 'Quarterly' then 'quarter' else 'month' end,p_month)::date;
 with facts as materialized (select f.*,case when f.room_number is null then 'Unassigned' else f.building_name||' / '||f.room_number end room from public.billing_facts(cutoff) f),
 periods as (select i,(anchor-make_interval(months=>i*step))::date start,(anchor+make_interval(months=>(1-i)*step))::date finish from generate_series(0,5) i),
 series as (select i,jsonb_build_object('label',case p_period when 'Yearly' then to_char(start,'YYYY') when 'Quarterly' then 'Q'||extract(quarter from start)||' '||extract(year from start) else to_char(start,'YYYY-MM') end,
   'value',(select coalesce(sum(amount),0) from public.payments where payment_date>=start and payment_date<finish and payment_date<=cutoff),
   'billed',(select coalesce(sum(total),0) from facts where billing_period>=start and billing_period<finish)) point from periods),
 forecast as (select i,jsonb_build_object('label',to_char(start,'YYYY-MM'),
   'value',(select coalesce(sum(amount),0) from public.payments where payment_date>=start and payment_date<(start+interval '1 month')::date and payment_date<=cutoff),
   'paymentCount',(select count(*) from public.payments where payment_date>=start and payment_date<(start+interval '1 month')::date and payment_date<=cutoff),
   'billed',(select coalesce(sum(total),0) from facts where billing_period>=start and billing_period<(start+interval '1 month')::date)) point
   from (select i,(date_trunc('month',p_month)-make_interval(months=>i))::date start from generate_series(0,2) i) m)
 select jsonb_build_object('snapshot',jsonb_build_object('cutoff',cutoff,
   'rows',coalesce((select jsonb_agg(to_jsonb(f) order by f.billing_period,f.id) from facts f where p_export or balance>0),'[]'::jsonb),
   'selected',coalesce((select jsonb_agg(to_jsonb(f) order by f.id) from facts f where to_char(billing_period,'YYYY-MM')=to_char(p_month,'YYYY-MM')),'[]'::jsonb),
   'late',coalesce((select jsonb_agg(to_jsonb(f) order by due_date,id) from facts f where status='Late'),'[]'::jsonb),
   'outstanding',(select coalesce(sum(balance),0) from facts),'overdue',(select coalesce(sum(balance),0) from facts where status='Late'),
   'collected',(select coalesce(sum(amount),0) from public.payments where payment_date>=date_trunc('month',p_month)::date and payment_date<=cutoff),
   'inventory',(select jsonb_agg(jsonb_build_object('status',s,'count',(select count(*) from public.rooms where status::text=s))) from unnest(array['occupied','available','reserved','maintenance']) s),
   'occupancy',(select coalesce(100.0*count(*) filter(where status='occupied')/nullif(count(*),0),0) from public.rooms),
   'activeTenants',(select count(distinct l.tenant_id) from public.leases l join public.profiles p on p.id=l.tenant_id where l.status='active' and p.role='tenant'),
   'available',(select count(*) from public.rooms where status='available'),'vacancyGap',(select coalesce(sum(monthly_rate),0) from public.rooms where status='available'),
   'openRequests',coalesce((select jsonb_agg(jsonb_build_object('id',id,'kind',kind,'status',status,'subject',subject,'created_at',created_at) order by created_at desc,id) from public.tenant_requests where status<>'Resolved'),'[]'::jsonb),
   'openInquiries',coalesce((select jsonb_agg(jsonb_build_object('id',id,'status',status,'created_at',created_at) order by created_at desc,id) from public.inquiries where status<>'Resolved'),'[]'::jsonb)),
   'series',(select jsonb_agg(point order by i desc) from series),'forecastMonths',(select jsonb_agg(point order by i desc) from forecast),
   'roomCount',(select count(*) from public.rooms)) into result;
 return result;
end $$;

revoke all on function public.billing_facts(date),public.tenant_directory_page(integer,integer),public.billing_page(date,text,text,text,integer,integer),public.analytics_report(date,text,boolean) from public,anon;
grant execute on function public.billing_facts(date),public.tenant_directory_page(integer,integer),public.billing_page(date,text,text,text,integer,integer),public.analytics_report(date,text,boolean) to authenticated;
