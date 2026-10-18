-- Add actionable collection-health measures to the administrator analytics report.
-- The report remains security invoker and verifies the caller is an administrator.
create or replace function public.analytics_report(p_month date,p_period text default 'Monthly',p_export boolean default false)
returns jsonb language plpgsql stable security invoker set search_path='' as $$
declare cutoff date; step integer; anchor date; result jsonb;
begin
 if not public.is_admin() then raise exception 'Administrator access required'; end if;
 if p_month is null or p_period not in ('Monthly','Quarterly','Yearly') then raise exception 'Invalid report period'; end if;
 cutoff:=least((date_trunc('month',p_month)+interval '1 month - 1 day')::date,(now() at time zone 'Asia/Manila')::date);
 step:=case p_period when 'Yearly' then 12 when 'Quarterly' then 3 else 1 end;
 anchor:=date_trunc(case p_period when 'Yearly' then 'year' when 'Quarterly' then 'quarter' else 'month' end,p_month)::date;
 with facts as materialized (
   select f.*,case when f.room_number is null then 'Unassigned' else f.building_name||' / '||f.room_number end room
   from public.billing_facts(cutoff) f
 ), punctual_facts as materialized (
   select f.*,coalesce((select sum(p.amount) from public.payments p
     where p.bill_id=f.id and p.payment_date<=least(f.due_date,cutoff)),0) paid_by_due
   from facts f
 ), periods as (
   select i,(anchor-make_interval(months=>i*step))::date start,(anchor+make_interval(months=>(1-i)*step))::date finish
   from generate_series(0,5) i
 ), series as (
   select i,jsonb_build_object('label',case p_period when 'Yearly' then to_char(start,'YYYY') when 'Quarterly' then 'Q'||extract(quarter from start)||' '||extract(year from start) else to_char(start,'YYYY-MM') end,
     'value',(select coalesce(sum(amount),0) from public.payments where payment_date>=start and payment_date<finish and payment_date<=cutoff),
     'billed',(select coalesce(sum(total),0) from facts where billing_period>=start and billing_period<finish)) point
   from periods
 ), forecast as (
   select i,jsonb_build_object('label',to_char(start,'YYYY-MM'),
     'value',(select coalesce(sum(amount),0) from public.payments where payment_date>=start and payment_date<(start+interval '1 month')::date and payment_date<=cutoff),
     'paymentCount',(select count(*) from public.payments where payment_date>=start and payment_date<(start+interval '1 month')::date and payment_date<=cutoff),
     'billed',(select coalesce(sum(total),0) from facts where billing_period>=start and billing_period<(start+interval '1 month')::date)) point
   from (select i,(date_trunc('month',p_month)-make_interval(months=>i))::date start from generate_series(0,2) i) m
 ), aging as (
   select 1 as position,'Current'::text as label,coalesce(sum(balance) filter(where balance>0 and due_date>=cutoff),0) balance,coalesce(count(*) filter(where balance>0 and due_date>=cutoff),0) bill_count from facts
   union all select 2,'1-7 days',coalesce(sum(balance) filter(where balance>0 and cutoff-due_date between 1 and 7),0),coalesce(count(*) filter(where balance>0 and cutoff-due_date between 1 and 7),0) from facts
   union all select 3,'8-30 days',coalesce(sum(balance) filter(where balance>0 and cutoff-due_date between 8 and 30),0),coalesce(count(*) filter(where balance>0 and cutoff-due_date between 8 and 30),0) from facts
   union all select 4,'31-60 days',coalesce(sum(balance) filter(where balance>0 and cutoff-due_date between 31 and 60),0),coalesce(count(*) filter(where balance>0 and cutoff-due_date between 31 and 60),0) from facts
   union all select 5,'61+ days',coalesce(sum(balance) filter(where balance>0 and cutoff-due_date>=61),0),coalesce(count(*) filter(where balance>0 and cutoff-due_date>=61),0) from facts
 ), punctuality as (
   select count(*) filter(where due_date<=cutoff) due_bill_count,
     count(*) filter(where due_date<=cutoff and paid_by_due>=total) on_time_bill_count,
     coalesce(100*sum(paid)/nullif(sum(total),0),0) collection_rate
   from punctual_facts
 ), repeat_late as (
   select tenant_id,max(name) name,string_agg(distinct room,', ') room,
     count(*) filter(where due_date between (cutoff-interval '6 months')::date and cutoff and paid_by_due<total) late_bill_count,
     coalesce(sum(balance) filter(where due_date<cutoff),0) overdue_balance
   from punctual_facts
   where tenant_id is not null
   group by tenant_id
   having count(*) filter(where due_date between (cutoff-interval '6 months')::date and cutoff and paid_by_due<total)>=2
 )
 select jsonb_build_object('snapshot',jsonb_build_object('cutoff',cutoff,
   'rows',coalesce((select jsonb_agg(to_jsonb(f) order by f.billing_period,f.id) from facts f where p_export or balance>0),'[]'::jsonb),
   'selected',coalesce((select jsonb_agg(to_jsonb(f) order by f.id) from facts f where to_char(billing_period,'YYYY-MM')=to_char(p_month,'YYYY-MM')),'[]'::jsonb),
   'late',coalesce((select jsonb_agg(to_jsonb(f) order by due_date,id) from facts f where status='Late'),'[]'::jsonb),
   'outstanding',(select coalesce(sum(balance),0) from facts),'overdue',(select coalesce(sum(balance),0) from facts where status='Late'),
   'collected',(select coalesce(sum(amount),0) from public.payments where payment_date>=date_trunc('month',p_month)::date and payment_date<=cutoff),
   'collectionHealth',jsonb_build_object(
     'aging',coalesce((select jsonb_agg(jsonb_build_object('label',label,'balance',balance,'billCount',bill_count) order by position) from aging),'[]'::jsonb),
     'dueBillCount',(select due_bill_count from punctuality),'onTimeBillCount',(select on_time_bill_count from punctuality),
     'onTimeRate',coalesce((select 100.0*on_time_bill_count/nullif(due_bill_count,0) from punctuality),0),
     'collectionRate',(select collection_rate from punctuality),
     'repeatLatePayers',coalesce((select jsonb_agg(jsonb_build_object('tenantId',tenant_id,'name',name,'room',room,'lateBillCount',late_bill_count,'overdueBalance',overdue_balance) order by late_bill_count desc,overdue_balance desc,name) from (select * from repeat_late order by late_bill_count desc,overdue_balance desc,name limit 10) repeat_late_page),'[]'::jsonb)),
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
