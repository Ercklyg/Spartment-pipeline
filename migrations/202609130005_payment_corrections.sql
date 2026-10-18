create table public.payment_corrections (
  id uuid primary key,
  payment_id uuid not null references public.payments(id) on delete cascade,
  corrected_by uuid references auth.users(id) on delete set null,
  reason text not null check(length(trim(reason)) between 1 and 500),
  previous_values jsonb not null,
  corrected_values jsonb not null,
  created_at timestamptz not null default now()
);
alter table public.payment_corrections enable row level security;
revoke all on public.payment_corrections from anon,authenticated;
grant select on public.payment_corrections to authenticated;
grant all on public.payment_corrections to service_role;
create policy "Admins read correction audit" on public.payment_corrections for select to authenticated using(public.is_admin());
create index payment_corrections_payment on public.payment_corrections(payment_id);

create function public.correct_bill_payment(p_id uuid,p_payment uuid,p_amount numeric,p_date date,p_reference text,p_reason text,p_expected jsonb)
returns uuid language plpgsql security definer set search_path='' as $$
declare original public.payments; bill public.bills; audit public.payment_corrections; paid numeric; total numeric; bill_id uuid; recipient uuid; old_values jsonb; new_values jsonb;
begin
  if not public.is_admin() then raise exception 'Administrator access required'; end if;
  if p_id is null or p_amount is null or p_amount<=0 or p_amount<>round(p_amount,2)
    or p_date is null or p_date>(now() at time zone 'Asia/Manila')::date
    or p_reason is null or length(trim(p_reason)) not between 1 and 500
    or length(coalesce(p_reference,''))>254 then raise exception 'Check amount, date, reference and correction reason'; end if;
  new_values:=jsonb_build_object('amount',p_amount,'payment_date',p_date,'reference_note',coalesce(p_reference,''));
  select p.bill_id into bill_id from public.payments p where p.id=p_payment;
  select * into bill from public.bills where id=bill_id for update;
  if not found then raise exception 'Payment bill not found'; end if;
  select * into original from public.payments where id=p_payment for update;
  select * into audit from public.payment_corrections where id=p_id;
  if found then
    if audit.payment_id=p_payment and audit.corrected_values=new_values and audit.reason=trim(p_reason) then return p_id; end if;
    raise exception 'Correction identifier already used';
  end if;
  old_values:=jsonb_build_object('amount',original.amount,'payment_date',original.payment_date,'reference_note',coalesce(original.reference_note,''));
  if p_expected is distinct from old_values then raise exception 'This payment changed. Refresh billing before correcting it'; end if;
  if new_values=old_values then raise exception 'Change at least one payment field'; end if;
  select coalesce(sum(amount),0) into paid from public.payments where payments.bill_id=bill.id and id<>p_payment;
  total:=bill.rent_amount+bill.electricity_amount+bill.water_amount;
  if p_amount>total-paid then raise exception 'Corrected payment exceeds bill balance'; end if;
  insert into public.payment_corrections(id,payment_id,corrected_by,reason,previous_values,corrected_values)
  values(p_id,p_payment,auth.uid(),trim(p_reason),old_values,new_values);
  update public.payments set amount=p_amount,payment_date=p_date,reference_note=coalesce(p_reference,'') where id=p_payment;
  update public.bills set status=case when paid+p_amount>=total then 'paid'::public.bill_status else 'partially_paid'::public.bill_status end where id=bill.id;
  select tenant_id into recipient from public.leases where id=bill.lease_id;
  insert into public.notifications(tenant_id,title,message,kind,source_id)
  values(recipient,'Payment corrected','Payment '||p_payment||' was corrected from PHP '||original.amount||' to PHP '||p_amount||'. Reason: '||trim(p_reason),'payment',p_payment);
  return p_id;
end $$;
revoke all on function public.correct_bill_payment(uuid,uuid,numeric,date,text,text,jsonb) from public,anon;
grant execute on function public.correct_bill_payment(uuid,uuid,numeric,date,text,text,jsonb) to authenticated;
