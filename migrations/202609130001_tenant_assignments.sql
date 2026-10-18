-- Requires the existing profiles(id, role, full_name) and rooms tables.
create unique index leases_one_active_room on public.leases(room_id) where status = 'active';
create unique index leases_one_active_tenant on public.leases(tenant_id) where status = 'active';
create table public.tenant_assignments (
  id uuid primary key references auth.users(id) on delete cascade,
  room_id uuid not null unique references public.rooms(id),
  full_name text not null,
  email text not null,
  phone text not null,
  monthly_rent numeric(12,2) not null check (monthly_rent > 0),
  move_in date not null,
  lease_end date check (lease_end >= move_in),
  emergency_name text not null,
  emergency_phone text not null,
  relationship text not null,
  created_at timestamptz not null default now()
);
create unique index tenant_assignments_email on public.tenant_assignments(lower(email));
alter table public.tenant_assignments enable row level security;
revoke all on public.tenant_assignments from anon, authenticated;
grant select on public.tenant_assignments to authenticated;
grant all on public.tenant_assignments to service_role;

create function public.tenant_admin() returns boolean
language sql stable security definer set search_path = ''
as $$ select exists(select 1 from public.profiles where id = auth.uid() and role = 'admin') $$;
revoke all on function public.tenant_admin() from public;
grant execute on function public.tenant_admin() to authenticated;
create policy tenant_assignments_read on public.tenant_assignments for select to authenticated
using (id = auth.uid() or public.tenant_admin());

-- Prevent profile role escalation even if an existing UPDATE policy permits it.
create function public.guard_profile_role() returns trigger
language plpgsql set search_path = '' as $$
begin
  if current_user in ('anon', 'authenticated') and
     ((TG_OP = 'INSERT' and new.role = 'admin') or
      (TG_OP = 'UPDATE' and new.role is distinct from old.role)) then
    raise exception 'Account roles can only be changed by the server';
  end if;
  return new;
end $$;
create trigger guard_profile_role before insert or update on public.profiles
for each row execute function public.guard_profile_role();

create function public.assign_tenant_room() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  perform 1 from public.rooms where id = new.room_id and status = 'available' for update;
  if not found then raise exception 'Room is no longer available'; end if;
  update public.rooms set status = 'occupied' where id = new.room_id;
  return new;
end $$;
create trigger assign_tenant_room before insert on public.tenant_assignments
for each row execute function public.assign_tenant_room();

create function public.guard_assigned_room() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  if new.status <> 'occupied' and exists(select 1 from public.tenant_assignments where room_id = new.id) then
    raise exception 'Room has an active tenant assignment';
  end if;
  return new;
end $$;
create trigger guard_assigned_room before update on public.rooms
for each row execute function public.guard_assigned_room();

create function public.release_tenant_room() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  update public.rooms set status = 'available' where id = old.room_id and status = 'occupied';
  return old;
end $$;
create trigger release_tenant_room after delete on public.tenant_assignments
for each row execute function public.release_tenant_room();

create function public.finish_tenant_account(account_id uuid, account jsonb) returns void
language plpgsql security definer set search_path = '' as $$
begin
  insert into public.profiles(id, role, full_name, phone)
  values(account_id, 'tenant', account->>'name', account->>'phone')
  on conflict(id) do update set role = 'tenant', full_name = excluded.full_name, phone = excluded.phone;
  insert into public.tenant_assignments(id, room_id, full_name, email, phone, monthly_rent,
    move_in, lease_end, emergency_name, emergency_phone, relationship)
  values(account_id, (account->>'room')::uuid, account->>'name', account->>'email',
    account->>'phone', (account->>'rent')::numeric, (account->>'moveIn')::date,
    nullif(account->>'leaseEnd','')::date, account->>'emergencyName',
    account->>'emergencyPhone', account->>'relationship');
  insert into public.leases(tenant_id, room_id, move_in_date, lease_end_date)
  values(account_id, (account->>'room')::uuid, (account->>'moveIn')::date,
    nullif(account->>'leaseEnd','')::date);
end $$;
revoke all on function public.finish_tenant_account(uuid,jsonb) from public, anon, authenticated;
grant execute on function public.finish_tenant_account(uuid,jsonb) to service_role;
