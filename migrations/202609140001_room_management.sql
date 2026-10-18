-- Existing "Admins manage rooms" RLS policy restricts these operations to admins.
-- Keep immutable identifiers, timestamps, and occupancy outside listing edits.
grant update (building_id, room_number, floor, size_sqm, monthly_rate, capacity, description)
  on public.rooms to authenticated;
grant delete on public.rooms to authenticated;

-- Existing room foreign keys intentionally prevent deletion of rooms referenced
-- by tenant assignments, leases, or tenant requests. Do not cascade their history.
