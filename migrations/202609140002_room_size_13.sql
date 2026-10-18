-- Support the larger rooms while retaining the existing minimum room size.
alter table public.rooms drop constraint rooms_size_sqm_check;
alter table public.rooms add constraint rooms_size_sqm_check
  check (size_sqm >= 8 and size_sqm <= 13);
