-- Backend-only storage. Recovery sessions contain AES-GCM ciphertext, not plain tokens.
create table public.password_recovery_sessions (
  ticket_hash text primary key check(length(ticket_hash)=64),
  encrypted_session text not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  expires_at timestamptz not null,
  busy boolean not null default false
);
create index password_recovery_expiry on public.password_recovery_sessions(expires_at);
create table public.password_recovery_limits (
  email_hash text primary key check(length(email_hash)=64),
  attempts integer not null,
  expires_at timestamptz not null
);
create index password_recovery_limit_expiry on public.password_recovery_limits(expires_at);
alter table public.password_recovery_sessions enable row level security;
alter table public.password_recovery_limits enable row level security;
revoke all on public.password_recovery_sessions,public.password_recovery_limits from public,anon,authenticated;
grant all on public.password_recovery_sessions,public.password_recovery_limits to service_role;

create function public.consume_password_recovery_attempt(p_email_hash text)
returns jsonb language plpgsql set search_path='' as $$
declare entry public.password_recovery_limits;
begin
  delete from public.password_recovery_limits where expires_at<=now();
  insert into public.password_recovery_limits(email_hash,attempts,expires_at)
    values(p_email_hash,1,now()+interval '15 minutes')
  on conflict(email_hash) do update set attempts=least(public.password_recovery_limits.attempts+1,4)
  returning * into entry;
  return jsonb_build_object('allowed',entry.attempts<=3,'expiresAt',entry.expires_at);
end $$;
revoke all on function public.consume_password_recovery_attempt(text) from public,anon,authenticated;
grant execute on function public.consume_password_recovery_attempt(text) to service_role;
