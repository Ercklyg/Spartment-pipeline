-- Event notifications commit or roll back with the underlying action.
alter table public.notifications add column kind text,
  add column source_id uuid;
revoke update on public.notifications from authenticated;
grant update(is_read) on public.notifications to authenticated;

create function public.notify_tenant_event() returns trigger
language plpgsql security definer set search_path='' as $$
declare recipient uuid; heading text; body text; event_kind text;
begin
  if TG_TABLE_NAME='bills' then
    select tenant_id into recipient from public.leases where id=new.lease_id;
    heading := 'New '||new.bill_type||' bill';
    body := 'Your '||new.bill_type||' bill for '||to_char(new.billing_period,'YYYY-MM')||' is available. Due '||new.due_date||'.';
    event_kind := 'bill';
  elsif TG_TABLE_NAME='payments' then
    select l.tenant_id into recipient from public.bills b join public.leases l on l.id=b.lease_id where b.id=new.bill_id;
    heading := 'Payment recorded';
    body := 'A payment of PHP '||to_char(new.amount,'FM999999999990.00')||' dated '||new.payment_date||' has been recorded.';
    event_kind := 'payment';
  else
    if new.status is not distinct from old.status and new.admin_note is not distinct from old.admin_note then return new; end if;
    recipient := new.tenant_id;
    heading := 'Request update: '||new.subject;
    body := 'Status: '||new.status||case when new.admin_note<>'' then '. '||new.admin_note else '' end;
    event_kind := 'request';
  end if;
  insert into public.notifications(tenant_id,title,message,kind,source_id)
  values(recipient,heading,body,event_kind,new.id);
  return new;
end $$;
create trigger notify_new_bill after insert on public.bills for each row execute function public.notify_tenant_event();
create trigger notify_new_payment after insert on public.payments for each row execute function public.notify_tenant_event();
create trigger notify_request_update after update on public.tenant_requests for each row execute function public.notify_tenant_event();
