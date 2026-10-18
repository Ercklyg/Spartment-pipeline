-- Financial writes go through checked functions, keeping payments and bill
-- status consistent even when multiple administrators record payments at once.
revoke insert, update, delete on public.bills, public.payments from authenticated, anon;

create function public.create_bill(p_lease uuid, p_type text, p_period date,
  p_due date, p_rent numeric, p_electricity numeric, p_water numeric)
returns uuid language plpgsql security definer set search_path = '' as $$
declare bill_id uuid;
begin
  if not public.is_admin() then raise exception 'Administrator access required'; end if;
  if p_type is null or p_type not in ('rent','utility') or p_period is null or p_due is null
    or p_rent is null or p_electricity is null or p_water is null
    or p_rent < 0 or p_electricity < 0 or p_water < 0
    or p_rent + p_electricity + p_water <= 0
    or p_rent + p_electricity + p_water > 9999999999.99
    or p_rent <> round(p_rent,2) or p_electricity <> round(p_electricity,2) or p_water <> round(p_water,2)
    or (p_type = 'rent' and (p_electricity <> 0 or p_water <> 0))
    or (p_type = 'utility' and p_rent <> 0) then
    raise exception 'Check bill type, dates and amounts';
  end if;
  perform 1 from public.leases where id = p_lease and status = 'active' for share;
  if not found then raise exception 'Select an active lease'; end if;
  insert into public.bills(lease_id,bill_type,billing_period,due_date,rent_amount,electricity_amount,water_amount)
  values(p_lease,p_type,date_trunc('month',p_period)::date,p_due,round(p_rent,2),round(p_electricity,2),round(p_water,2)) returning id into bill_id;
  return bill_id;
end $$;
revoke all on function public.create_bill(uuid,text,date,date,numeric,numeric,numeric) from public,anon;
grant execute on function public.create_bill(uuid,text,date,date,numeric,numeric,numeric) to authenticated;

create function public.record_bill_payment(p_bill uuid, p_id uuid, p_amount numeric, p_date date, p_reference text)
returns uuid language plpgsql security definer set search_path = '' as $$
declare bill public.bills; existing public.payments; total numeric; paid numeric;
begin
  if not public.is_admin() then raise exception 'Administrator access required'; end if;
  if p_id is null or p_amount is null or p_amount <= 0 or p_amount <> round(p_amount,2)
    or p_date is null or p_date > (now() at time zone 'Asia/Manila')::date
    or length(coalesce(p_reference,'')) > 254 then raise exception 'Check payment amount, date and reference'; end if;
  select * into bill from public.bills where id=p_bill for update;
  if not found then raise exception 'Bill not found'; end if;
  select * into existing from public.payments where id=p_id;
  if found then
    if existing.bill_id=p_bill and existing.amount=p_amount and existing.payment_date=p_date
      and coalesce(existing.reference_note,'')=coalesce(p_reference,'') then return p_id; end if;
    raise exception 'Payment reference was already used for a different payment';
  end if;
  total := bill.rent_amount+bill.electricity_amount+bill.water_amount;
  select coalesce(sum(amount),0) into paid from public.payments where bill_id=p_bill;
  if p_amount > total-paid then raise exception 'Payment exceeds remaining balance'; end if;
  insert into public.payments(id,bill_id,amount,payment_date,reference_note) values(p_id,p_bill,p_amount,p_date,p_reference);
  update public.bills set status=case when paid+p_amount>=total then 'paid'::public.bill_status
    else 'partially_paid'::public.bill_status end where id=p_bill;
  return p_id;
end $$;
revoke all on function public.record_bill_payment(uuid,uuid,numeric,date,text) from public,anon;
grant execute on function public.record_bill_payment(uuid,uuid,numeric,date,text) to authenticated;

create table public.tenant_requests (
  id uuid primary key,
  tenant_id uuid not null references public.profiles(id) on delete cascade,
  room_id uuid references public.rooms(id),
  kind text not null check(kind in ('maintenance','other')),
  subject text not null check(length(trim(subject)) between 1 and 160),
  message text not null check(length(trim(message)) between 1 and 4000),
  priority text not null default 'Normal' check(priority in ('Normal','High','Urgent')),
  status text not null default 'Open' check(status in ('Open','Scheduled','In progress','Resolved')),
  admin_note text not null default '' check(length(admin_note)<=4000),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
alter table public.tenant_requests enable row level security;
revoke all on public.tenant_requests from anon,authenticated;
grant select on public.tenant_requests to authenticated;
grant update(status,admin_note) on public.tenant_requests to authenticated;
grant all on public.tenant_requests to service_role;
create policy "Read own requests or admin" on public.tenant_requests for select to authenticated
using(tenant_id=auth.uid() or public.is_admin());
create policy "Admins update requests" on public.tenant_requests for update to authenticated
using(public.is_admin()) with check(public.is_admin());
create trigger tenant_request_updated before update on public.tenant_requests
for each row execute function public.set_updated_at();
create index tenant_requests_tenant on public.tenant_requests(tenant_id,created_at desc);

create function public.submit_tenant_request(p_id uuid,p_kind text,p_subject text,p_message text,p_priority text)
returns uuid language plpgsql security definer set search_path='' as $$
declare assigned_room uuid; previous public.tenant_requests;
begin
  if not exists(select 1 from public.profiles where id=auth.uid() and role='tenant') then
    raise exception 'Sign in with a tenant account to submit a request'; end if;
  select * into previous from public.tenant_requests where id=p_id;
  if found then
    if previous.tenant_id=auth.uid() and previous.kind=p_kind and previous.subject=trim(p_subject)
      and previous.message=trim(p_message) and previous.priority=p_priority then return p_id; end if;
    raise exception 'Request identifier already used';
  end if;
  select room_id into assigned_room from public.leases where tenant_id=auth.uid() and status='active';
  if p_kind='maintenance' and assigned_room is null then raise exception 'An active room assignment is required'; end if;
  insert into public.tenant_requests(id,tenant_id,room_id,kind,subject,message,priority)
  values(p_id,auth.uid(),assigned_room,p_kind,trim(p_subject),trim(p_message),p_priority);
  return p_id;
end $$;
revoke all on function public.submit_tenant_request(uuid,text,text,text,text) from public,anon;
grant execute on function public.submit_tenant_request(uuid,text,text,text,text) to authenticated;

alter table public.inquiries add column preferred_room text check(length(preferred_room)<=160),
  add column move_in_date date,
  add column status text not null default 'Open' check(status in ('Open','Resolved'));
drop policy "Anyone can submit an inquiry" on public.inquiries;
create policy "Anyone can submit an inquiry" on public.inquiries for insert to anon,authenticated
with check(status='Open' and length(trim(full_name)) between 1 and 160
  and length(email) between 3 and 254 and email ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  and length(coalesce(phone,''))<=40 and length(coalesce(message,''))<=4000
  and (move_in_date is null or move_in_date >= (now() at time zone 'Asia/Manila')::date));
