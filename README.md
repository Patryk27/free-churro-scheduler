# free-churro-scheduler

Free Churro Scheduler: it's the task queue, stupid!™

## Usage

Step 1: Start a Postgres database - if you've got Nix, it's as simple as:

```
$ nix develop
$ initdb
$ echo 'create database db' | postgres --single -E postgres
$ postgres -k ''
```

Step 2: Initialize the database and start the supervisor

```
$ ./fcs init --database postgres://127.0.0.1:5432/db
$ ./fcs supervise --database postgres://127.0.0.1:5432/db 
```

Step 3: Start one (or more) workers

```
$ ./fcs \
    work \
    --database postgres://127.0.0.1:5432/db \
    --listen 127.0.0.1:5000 \
    --id 205109a7-bcd4-4106-a960-ab45b4c42df8
```

(`--id` has to be unique for each worker - it can be generated e.g. through
`uuidgen`)

Step 4: Have fun!

```
curl \
    -X POST \
    localhost:5000/tasks \
    -H 'Content-Type: application/json' \
    -d '{ "def": { "ty": "baz" } }'
```

```
curl localhost:5000/tasks | jq '.'
```

## Implementation

FCS consists of one supervisor node and 1+ worker nodes - all of them are
connected to the same Postgres database, a database which serves both as a data
storage and, through its notification mechanism, as a convenient message relay.

When a new task is created, it's added into the `tasks` table and then a
`hey there's a new task` message is sent to the supervisor which then
dispatches this task to a specific worker.

Supervisor, through heartbeat messages, knows how many workers are there and
which workers are (most likely) alive - it also keeps track of busy-ness of
each worker so that a message is sent only to a random worker that's known to
be unoccupied.

Supervisor takes care of the scheduling as well, i.e. when a task is set to be
executed some time in the future (as opposed to "asap"), it's the supervisor
which is keeping the tabs on that (through a binary heap).

FCS guarantees _at most once_ delivery through the use of database and
transactions - in particular even if supervisor faults out and assigns the same
task to a couple of workers, only one of them will actually execute the task,
because doing so requires calling `Database::begin_task()` which works as an
atomic CAS.

## Alternative implementations

### Automatically-elected Supervisors

I'm a fan of plug-and-play solutions, so having a dedicated supervisor node rubs
me in a wrong way, but it's also a relatively straightforward to understand, so
there's that.

In principle it _would_ be possible for the network to automatically "elevate"
a certain worker node to fulfill both the role of a worker and the supervisor¹,
but that touches a rather difficult subject (consensus algorithms) and I've
decided there's no reason to go _this_ wild.

¹ the cluster could agree that the worker node with the lowest id is also the
  supervisor etc. etc.

### Hash Hash Hash

One of the supervisor's roles is to dispatch the tasks between workers in a fair
and uniform way - but if we squint our eyes a bit, this doesn't actually require
a whole separate node dedicated just for that.

Currently creating a task adds an entry into a database table and then sends a
message to the supervisor that says `please take care of scheduling that` - but
if every worker knew every other worker, we could instead change the logic to:

```
let id = database::create_task(/* ... */);
let elected_worker_id = id.hash() % known_workers.len();

send_message(
    workers[elected_worker_id],
    "please start working on task ${id}"
);
```

This way the load would get evenly distributed across the entire cluster, even
if one worker (i.e. one http server) happens to retrieve most of the edge
traffic.

In fact, this design is something that I've started with, but unfortunately it
falls short on edge cases - even if this design doesn't require a dedicated
supervisor-scheduler node, it requires a _reaper node_ that would be able to
reassign tasks from workers that crashed / went offline.

In principle we could make each worker node also a reaper node¹, solving this
issue, but that once again touches the topic of consensus algorithms which I've
decided to stick away from.

¹ we could say that worker node #N is the reaper for node #N+1, worker node #N+1
  is the reaper for node #N+2 etc.
