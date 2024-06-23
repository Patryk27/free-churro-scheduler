create table workers (
    id uuid not null primary key,
    last_heard_at timestamptz not null
);

create type task_status as enum (
    'pending',
    'dispatched',
    'running',
    'succeeded',
    'failed',
    'interrupted'
);

create table tasks (
    id uuid not null primary key,
    def json not null,
    worker_id uuid,
    status task_status not null,
    created_at timestamptz not null,
    updated_at timestamptz not null,
    scheduled_at timestamptz,

    foreign key (worker_id) references workers (id)
);

create index on tasks (worker_id);
create index on tasks (status);
