import bravo
import bravo/uset
import gleam/deque
import gleam/dict.{type Dict}
import gleam/dynamic
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/result
import gleam/string
import lamb
import lamb/query

type PoolSubject(resource_type) =
  process.Subject(PoolMsg(resource_type))

/// A resource pool.
pub opaque type Pool(resource_type) {
  Pool(
    size: Int,
    subject: PoolSubject(resource_type),
    supervisor: process.Pid,
    resource_shutdown_function: fn(resource_type) -> Nil,
  )
}

type LiveWorker(resource_type) {
  LiveWorker(
    // worker_pid: process.Pid,
    worker: ResourceSubject(resource_type),
    worker_monitor: process.ProcessMonitor,
    caller: process.Pid,
  )
}

type LiveWorkers(resource_type) =
  Dict(process.Pid, LiveWorker(resource_type))

/// An error returned when creating a [`Pool`](#Pool).
pub type InitError(resource_create_error) {
  /// The pool actor failed to start.
  PoolStartError(actor.StartError)
  /// The monitor actor failed to start.
  MonitorStartError(actor.StartError)
  /// A worker actor failed to start.
  WorkerStartError(actor.StartError)
  /// The resource creation function failed.
  ResourceCreateError(resource_create_error)
  // TableCreateError(lamb.Error)
  /// ETS table creation failed.
  TableCreateError(bravo.BravoError)
  /// The supervisor failed to start.
  SupervisorStartError(dynamic.Dynamic)
}

/// An error returned when the resource pool fails to shut down.
pub type ShutdownError {
  /// There are still resources checked out. Ignore this failure case by
  /// calling [`force_shutdown`](#force_shutdown) function.
  ResourcesInUse(remaining: Int)
  /// The shutdown timeout expired.
  ShutdownTimeout
  /// The pool was already down or failed to send the response message.
  CalleeDown(reason: dynamic.Dynamic)
}

/// An error returned when failing to apply a function to a pooled resource.
pub type ApplyError {
  /// There are no resources available in the pool.
  NoResourcesAvailable
  /// The checkout timeout expired.
  CheckoutTimeout
  /// The worker failed to be called.
  WorkerCallError(process.CallError(dynamic.Dynamic))
}

/// Start a new resource pool.
///
/// ```gleam
/// // Creates a pool with 10 strings.
/// let assert Ok(pool) = bath.init(10, fn() { Ok("Some pooled resource") })
/// ```
pub fn init(
  size: Int,
  resource_create_function: fn() -> Result(resource_type, resource_create_error),
  resource_shutdown_function: fn(resource_type) -> Nil,
  pool_init_timeout: Int,
  worker_init_timeout: Int,
) -> Result(Pool(resource_type), InitError(resource_create_error)) {
  // use live_workers <- result.try(
  //   // lamb.create(
  //   //   name: "bath_live_workers",
  //   //   access: lamb.Public,
  //   //   kind: lamb.Set,
  //   //   registered: False,
  //   // )
  //   // TODO: use random name
  //   uset.new(name: "bath_live_workers", access: bravo.Public, keypos: 1)
  //   |> result.map_error(TableCreateError),
  // )

  let live_workers = dict.new()

  let actor_result =
    actor.start_spec(pool_spec(live_workers, pool_init_timeout))
    |> result.map_error(PoolStartError)

  use subject <- result.try(actor_result)

  let workers_result =
    list.repeat("", size)
    |> list.try_map(fn(_) {
      use subject <- result.try(
        actor.start_spec(worker_spec(
          resource_create_function,
          resource_shutdown_function,
          subject,
          worker_init_timeout,
        ))
        |> result.map_error(WorkerStartError),
      )
      Ok(subject)
    })

  use workers <- result.try(workers_result)

  let sup =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.auto_shutdown(static_supervisor.AllSignificant)
  let sup_result =
    workers
    |> list.index_fold(sup, fn(sup, actor, idx) {
      static_supervisor.add(
        sup,
        static_supervisor.worker_child("worker_" <> int.to_string(idx), fn() {
          process.subject_owner(actor) |> Ok
        })
          |> static_supervisor.significant(True)
          |> static_supervisor.restart(static_supervisor.Transient),
      )
    })
    |> static_supervisor.start_link()
    |> result.map_error(SupervisorStartError)

  use sup <- result.try(sup_result)

  Ok(Pool(size:, subject:, supervisor: sup, resource_shutdown_function:))
}

/// Check out a resource from the pool, apply the `next` function, then check
/// the resource back in.
///
/// ```gleam
/// let assert Ok(pool) = bath.init(10, fn() { Ok("Some pooled resource") })
///
/// use resource <- bath.apply(pool, 1000)
/// // Do stuff with resource...
/// ```
pub fn apply(
  pool: Pool(resource_type),
  timeout: Int,
  next: fn(resource_type) -> result_type,
) -> Result(result_type, ApplyError) {
  let self = process.self()
  use worker <- result.try(check_out(pool.subject, self, timeout))
  let next = unsafe_coerce_to_dynamic_function(next)

  let result =
    process.try_call(worker, UseResource(next, _), timeout)
    |> result.map_error(WorkerCallError)

  check_in(pool.subject, worker)

  use return_value <- result.try(result)
  Ok(unsafe_coerce_to_return_type(return_value))
}

/// Shut down the pool, calling the `resource_shutdown_function` on each
/// resource in the pool.
///
/// Will fail if there are still resources checked out.
pub fn shutdown(pool: Pool(resource_type), timeout: Int) {
  process.send_exit(pool.supervisor)
  Ok(Nil)
  // process.try_call(
  //   pool.subject,
  //   Shutdown(pool.resource_shutdown_function, pool.supervisor, _),
  //   timeout,
  // )
  // |> result.map_error(fn(err) {
  //   case err {
  //     process.CallTimeout -> ShutdownTimeout
  //     process.CalleeDown(reason) -> CalleeDown(reason:)
  //   }
  // })
  // |> result.flatten
}

/// Shut down the pool, calling the `resource_shutdown_function` on each
/// resource in the pool.
///
/// Will not fail, even if resources are checked out, and will call the
/// `resource_shutdown_function` on both checked in and checked out resources.
pub fn force_shutdown(
  pool: Pool(resource_type),
  resource_shutdown_function: fn(resource_type) -> Nil,
) {
  process.send(
    pool.subject,
    ForceShutdown(resource_shutdown_function, pool.supervisor),
  )
}

fn check_out(
  pool_subject: PoolSubject(resource_type),
  caller: process.Pid,
  timeout: Int,
) {
  process.try_call(pool_subject, CheckOut(_, caller:), timeout)
  |> result.replace_error(CheckoutTimeout)
  |> result.flatten
}

fn check_in(
  pool_subject: PoolSubject(resource_type),
  worker: ResourceSubject(resource_type),
) -> Nil {
  process.send(pool_subject, CheckIn(worker))
}

// ----- Pool actor ----- //

type PoolState(resource_type) {
  PoolState(
    workers: deque.Deque(ResourceSubject(resource_type)),
    // live_workers: lamb.Table(process.Pid, LiveWorker(resource_type)),
    // live_workers: uset.USet(LiveWorker(resource_type)),
    live_workers: LiveWorkers(resource_type),
    // supervisor: process.Pid,
    selector: process.Selector(PoolMsg(resource_type)),
  )
}

type PoolMsg(resource_type) {
  ProcessDown(process.ProcessDown)
  CheckIn(worker: ResourceSubject(resource_type))
  CheckOut(
    reply_to: process.Subject(
      Result(ResourceSubject(resource_type), ApplyError),
    ),
    caller: process.Pid,
  )
  Shutdown(
    resource_shutdown_function: fn(resource_type) -> Nil,
    supervisor: process.Pid,
    reply_to: process.Subject(Result(Nil, ShutdownError)),
  )
  ForceShutdown(
    resource_shutdown_function: fn(resource_type) -> Nil,
    supervisor: process.Pid,
  )
}

fn handle_pool_message(
  msg: PoolMsg(resource_type),
  pool_state: PoolState(resource_type),
) {
  case msg {
    CheckIn(worker:) -> {
      let new_workers = deque.push_back(pool_state.workers, worker)

      // let query =
      //   query.new()
      //   |> query.index(worker |> process.subject_owner)

      // lamb.remove(pool_state.live_workers, query)

      // uset.delete_key(pool_state.live_workers, worker |> process.subject_owner)
      let selector = case
        dict.get(pool_state.live_workers, worker |> process.subject_owner)
      {
        Ok(live_worker) -> {
          // Demonitor the process
          let selector =
            pool_state.selector
            |> process.deselecting_process_down(live_worker.worker_monitor)

          process.demonitor_process(live_worker.worker_monitor)

          selector
        }
        Error(_) -> pool_state.selector
      }

      let live_workers =
        dict.delete(pool_state.live_workers, worker |> process.subject_owner)

      actor.with_selector(
        actor.continue(PoolState(workers: new_workers, live_workers:, selector:)),
        selector,
      )
    }
    CheckOut(reply_to:, caller:) -> {
      case deque.pop_front(pool_state.workers) {
        Ok(#(worker, new_workers)) -> {
          // Add the worker to the live_workers table
          let worker_pid = process.subject_owner(worker)
          let worker_monitor = process.monitor_process(worker_pid)

          // lamb.insert(
          //   pool_state.live_workers,
          //   worker |> process.subject_owner,
          //   LiveWorker(worker:, worker_monitor:, caller:),
          // )
          // uset.insert(pool_state.live_workers, [
          //   LiveWorker(worker_pid:, worker:, worker_monitor:, caller:),
          // ])
          let live_workers =
            dict.insert(
              pool_state.live_workers,
              worker |> process.subject_owner,
              LiveWorker(worker:, worker_monitor:, caller:),
            )

          actor.send(reply_to, Ok(worker))

          io.debug("live workers: check out")
          io.debug(pool_state.live_workers)

          let selector =
            pool_state.selector
            |> process.selecting_process_down(worker_monitor, ProcessDown)

          actor.with_selector(
            actor.continue(PoolState(
              workers: new_workers,
              selector:,
              live_workers:,
            )),
            selector,
          )
        }
        Error(_) -> {
          actor.send(reply_to, Error(NoResourcesAvailable))
          actor.continue(pool_state)
        }
      }
    }
    Shutdown(resource_shutdown_function:, supervisor:, reply_to:) -> {
      io.debug("Shutdown")
      case dict.size(pool_state.live_workers) {
        0 -> {
          pool_state.workers
          |> deque.to_list
          |> list.each(actor.send(_, ShutdownResource(
            resource_shutdown_function,
            supervisor:,
          )))

          // TODO: handle this better
          // process.kill(supervisor)

          process.send(reply_to, Ok(Nil))
          actor.Stop(process.Normal)
        }
        remaining -> {
          process.send(reply_to, Error(ResourcesInUse(remaining:)))
          actor.continue(pool_state)
        }
      }
    }
    ForceShutdown(resource_shutdown_function:, supervisor:) -> {
      // static_super
      // list.append(pool_state.checked_in, pool_state.checked_out)
      // |> list.each(resource_shutdown_function)

      // TODO: handle this better
      // process.kill(supervisor)

      actor.Stop(process.Normal)
    }
    ProcessDown(process_down) -> {
      // TODO: remove let_assert
      // let assert [live_worker] =
      //   lamb.lookup(pool_state.live_workers, process_down.pid)
      // let assert Ok(live_worker) =
      //   uset.lookup(pool_state.live_workers, process_down.pid)
      io.debug("Pool live workers")
      io.debug(pool_state.live_workers)
      let #(workers, selector) = case
        dict.get(pool_state.live_workers, process_down.pid)
      {
        Ok(live_worker) -> {
          // Demonitor the process
          let selector =
            pool_state.selector
            |> process.deselecting_process_down(live_worker.worker_monitor)

          process.demonitor_process(live_worker.worker_monitor)

          // Remove the worker from the workers deque
          let new_workers =
            pool_state.workers
            |> deque.to_list
            |> list.filter(fn(worker) { worker != live_worker.worker })
            |> deque.from_list

          #(new_workers, selector)
        }
        Error(_) -> #(pool_state.workers, pool_state.selector)
      }

      // Remove the process from the live_workers set
      // let query = query.new() |> query.index(process_down.pid)
      // lamb.remove(pool_state.live_workers, query)
      // uset.delete_key(pool_state.live_workers, process_down.pid)

      let live_workers = dict.delete(pool_state.live_workers, process_down.pid)

      actor.with_selector(
        actor.continue(PoolState(workers:, selector:, live_workers:)),
        pool_state.selector,
      )
    }
  }
}

fn pool_spec(
  live_workers: LiveWorkers(resource_type),
  init_timeout: Int,
) -> actor.Spec(PoolState(resource_type), PoolMsg(resource_type)) {
  actor.Spec(
    init: fn() {
      let selector = process.new_selector()
      actor.Ready(PoolState(deque.new(), selector:, live_workers:), selector)
    },
    init_timeout:,
    loop: handle_pool_message,
  )
}

// ----- Resource actor ----- //

type ResourceMessage(resource_type) {
  UseResource(
    next: fn(resource_type) -> dynamic.Dynamic,
    reply_to: process.Subject(dynamic.Dynamic),
  )
  // Takes a shutdown function
  ShutdownResource(
    resource_shutdown_function: fn(resource_type) -> Nil,
    supervisor: process.Pid,
  )
  ResourceExit(
    process.ExitMessage,
    resource_shutdown_function: fn(resource_type) -> Nil,
  )
}

type ResourceSubject(resource_type) =
  process.Subject(ResourceMessage(resource_type))

fn worker_spec(
  resource_create_function: fn() -> Result(resource_type, resource_create_error),
  resource_shutdown_function: fn(resource_type) -> Nil,
  pool_subject: PoolSubject(resource_type),
  init_timeout: Int,
) -> actor.Spec(resource_type, ResourceMessage(resource_type)) {
  actor.Spec(
    init: fn() {
      case resource_create_function() {
        Ok(resource) -> {
          // Check in the worker
          let self = process.new_subject()
          process.send(pool_subject, CheckIn(self))
          // process.trap_exits(True)

          let selector =
            process.new_selector()
            |> process.selecting(self, function.identity)
            |> process.selecting_trapped_exits(ResourceExit(
              _,
              resource_shutdown_function,
            ))

          actor.Ready(resource, selector)
        }
        Error(resource_create_error) -> {
          actor.Failed(resource_create_error |> string.inspect)
        }
      }
    },
    init_timeout:,
    loop: handle_resource_message,
  )
}

fn handle_resource_message(
  msg: ResourceMessage(resource_type),
  resource: resource_type,
) {
  case msg {
    UseResource(next:, reply_to:) -> {
      actor.send(reply_to, next(resource))
      actor.continue(resource)
    }
    ShutdownResource(resource_shutdown_function:, supervisor:) -> {
      io.debug("Worker shutting down")
      resource_shutdown_function(resource)
      actor.Stop(process.Normal)
    }
    ResourceExit(exit_message, resource_shutdown_function:) -> {
      io.debug("exit")
      io.debug(exit_message)
      resource_shutdown_function(resource)
      actor.Stop(process.Normal)
    }
  }
}

// ----- Utils ----- //

@external(erlang, "bath_ffi", "unsafe_coerce")
fn unsafe_coerce_to_dynamic_function(
  next: fn(resource_type) -> return_type,
) -> fn(resource_type) -> dynamic.Dynamic

@external(erlang, "bath_ffi", "unsafe_coerce")
fn unsafe_coerce_to_return_type(term: dynamic.Dynamic) -> return_type
