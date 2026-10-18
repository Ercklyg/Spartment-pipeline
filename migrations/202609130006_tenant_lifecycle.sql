-- Keep accounts, leases and bills after move-out; archive the assignment.
alter table public.tenant_assignments add column status text not null default 'active'
  check (status in ('active','ended'));
alter table public.tenant_assignments alter column room_id drop not null;
alter table public.tenant_assignments add constraint assignment_status_room
  check ((status='active' and room_id is not null) or (status='ended' and room_id is null));

create table public.tenant_occupancy_events (
  id uuid primary key,
  tenant_id uuid not null references public.profiles(id),
  previous_lease_id uuid not null references public.leases(id),
  new_lease_id uuid references public.leases(id),
  performed_by uuid not null references public.profiles(id),
  details jsonb not null,
  created_at timestamptz not null default now()
);
alter table public.tenant_occupancy_events enable row level security;
revoke all on public.tenant_occupancy_events from anon,authenticated;
grant select on public.tenant_occupancy_events to authenticated;
grant all on public.tenant_occupancy_events to service_role;
create policy occupancy_event_read on public.tenant_occupancy_events for select to authenticated
  using (public.is_admin() or tenant_id=auth.uid());

create function public.change_tenant_occupancy(p_id uuid,p_tenant uuid,p_lease uuid,
  p_date date,p_room uuid default null,p_rent numeric default null,p_end date default null)
returns jsonb language plpgsql security definer set search_path='' as $$
declare previous public.leases; assignment public.tenant_assignments; event public.tenant_occupancy_events;
  new_lease uuid; payload jsonb; target_status public.room_status;
begin
  if not public.is_admin() then raise exception 'Administrator access required'; end if;
  if p_id is null or p_tenant is null or p_lease is null or p_date is null
    or p_date>(now() at time zone 'Asia/Manila')::date then raise exception 'Check operation ID, tenant, lease and effective date'; end if;
  if p_room is not null and (p_rent is null or p_rent<=0 or p_rent>9999999999.99
    or p_rent<>round(p_rent,2) or (p_end is not null and p_end<p_date)) then raise exception 'Check rent and lease end date'; end if;
  if p_room is null and (p_rent is not null or p_end is not null) then raise exception 'Move-out does not accept a new rent or lease end'; end if;
  payload:=jsonb_build_object('date',p_date,'room',p_room,'rent',p_rent,'end',p_end);
  -- Serializes retries and changes for one tenant before checking the active lease.
  select * into assignment from public.tenant_assignments where id=p_tenant for update;
  if not found then raise exception 'Tenant assignment not found'; end if;
  select * into event from public.tenant_occupancy_events where id=p_id;
  if found then
    if event.tenant_id=p_tenant and event.previous_lease_id=p_lease and event.details=payload then
      return jsonb_build_object('ok',true,'leaseId',event.new_lease_id,'eventId',event.id);
    end if;
    raise exception 'Operation ID already used for a different change';
  end if;
  select * into previous from public.leases where id=p_lease and tenant_id=p_tenant and status='active' for update;
  if not found or assignment.status<>'active' or assignment.room_id<>previous.room_id then raise exception 'The active lease changed. Refresh the tenant before retrying'; end if;
  if p_date<previous.move_in_date then raise exception 'Effective date cannot be before move-in'; end if;
  if p_room=previous.room_id then raise exception 'Choose a different room'; end if;
  perform 1 from public.rooms where id in (previous.room_id,p_room) order by id for update;
  if p_room is not null then
    select status into target_status from public.rooms where id=p_room;
    if not found or target_status<>'available' or exists(select 1 from public.tenant_assignments where room_id=p_room) then raise exception 'The destination room is not available'; end if;
  end if;
  update public.leases set status='ended',lease_end_date=p_date where id=previous.id;
  if p_room is null then
    update public.tenant_assignments set room_id=null,status='ended',lease_end=p_date where id=p_tenant;
  else
    insert into public.leases(tenant_id,room_id,move_in_date,lease_end_date)
      values(p_tenant,p_room,p_date,p_end) returning id into new_lease;
    update public.tenant_assignments set room_id=p_room,monthly_rent=p_rent,move_in=p_date,lease_end=p_end where id=p_tenant;
    update public.rooms set status='occupied' where id=p_room;
  end if;
  update public.rooms set status='available' where id=previous.room_id;
  insert into public.tenant_occupancy_events(id,tenant_id,previous_lease_id,new_lease_id,performed_by,details)
    values(p_id,p_tenant,previous.id,new_lease,auth.uid(),payload);
  insert into public.notifications(tenant_id,title,message,kind,source_id)
    values(p_tenant,case when p_room is null then 'Move-out recorded' else 'Room transfer recorded' end,
      'Your tenancy was updated effective '||p_date||'. Previous bills and payments remain available.','tenancy',p_id);
  return jsonb_build_object('ok',true,'leaseId',new_lease,'eventId',p_id);
end $$;
revoke all on function public.change_tenant_occupancy(uuid,uuid,uuid,date,uuid,numeric,date) from public,anon;
grant execute on function public.change_tenant_occupancy(uuid,uuid,uuid,date,uuid,numeric,date) to authenticated;
