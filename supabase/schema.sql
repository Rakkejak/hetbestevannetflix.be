create table if not exists public.boutique_preferences (
  user_id uuid not null references auth.users(id) on delete cascade,
  title_key text not null,
  status text not null check (status in ('seen', 'dismissed')),
  updated_at timestamptz not null default now(),
  primary key (user_id, title_key)
);

alter table public.boutique_preferences enable row level security;

create policy "Users can read their own boutique preferences"
on public.boutique_preferences for select
using (auth.uid() = user_id);

create policy "Users can add their own boutique preferences"
on public.boutique_preferences for insert
with check (auth.uid() = user_id);

create policy "Users can update their own boutique preferences"
on public.boutique_preferences for update
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "Users can delete their own boutique preferences"
on public.boutique_preferences for delete
using (auth.uid() = user_id);
