-- Existing room policies expose available rooms and admin reads only.
-- Allow tenants to resolve the occupied room belonging to their active lease.
create policy "Tenants read their active room" on public.rooms
for select to authenticated
using (exists (
  select 1 from public.leases
  where leases.room_id = rooms.id
    and leases.tenant_id = auth.uid()
    and leases.status = 'active'
));
