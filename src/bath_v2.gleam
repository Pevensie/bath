import gleam/deque
import gleam/dict.{type Dict}
import gleam/dynamic
import gleam/erlang/process.{type Pid, type Subject}
import gleam/function
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/otp/static_supervisor as sup
import gleam/result
import gleam/string

// ---- Pool config ----- //

pub type Strategy {
  FIFO
  LIFO
}

pub opaque type PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: Int,
    create_resource: fn() -> Result(resource_type, resource_create_error),
    shutdown_resource: fn(resource_type) -> Nil,
    strategy: Strategy,
  )
}

pub fn new(
  resource create_resource: fn() -> Result(resource_type, resource_create_error),
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: 10,
    create_resource: create_resource,
    shutdown_resource: fn(_) { Nil },
    strategy: FIFO,
  )
}

pub fn with_size(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  size size: Int,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, size:)
}

pub fn with_shutdown(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  shutdown shutdown_resource: fn(resource_type) -> Nil,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, shutdown_resource:)
}

pub fn with_strategy(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  strategy strategy: Strategy,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, strategy:)
}

// ----- Lifecycle functions ---- //

pub type StartError {
  PoolActorStartError(actor.StartError)
  WorkerStartError(actor.StartError)
  PoolSupervisorStartError(dynamic.Dynamic)
  WorkerSupervisorStartError(dynamic.Dynamic)
}

pub type ApplyError {
  NoResourcesAvailable
  CheckOutTimeout
  WorkerCallTimeout
  WorkerCrashed(process.ProcessDown)
}

pub fn start(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  timeout init_timeout: Int,
  // TODO: errors
) -> Result(Pool(resource_type), StartError) {
  // The supervision tree for pools looks like this:
  // supervisor (probably rest for 1?)
  // |        |
  // |        |
  // pool  supervisor (one for one)
  //        |  |  |
  //       /   |   \
  //      /    |    \
  // worker  worker  worker

  let main_supervisor = sup.new(sup.RestForOne)
  let worker_supervisor = sup.new(sup.OneForOne)

  let pool_start_result =
    actor.start_spec(pool_spec(pool_config, init_timeout))
    |> result.map_error(PoolActorStartError)

  use pool_subject <- result.try(pool_start_result)

  let workers_result =
    list.repeat("", pool_config.size)
    |> list.try_map(fn(_) {
      use subject <- result.try(
        actor.start_spec(worker_spec(
          pool_subject,
          pool_config.create_resource,
          pool_config.shutdown_resource,
          init_timeout,
        ))
        |> result.map_error(WorkerStartError),
      )
      Ok(subject)
    })

  use workers <- result.try(workers_result)

  // Add workers to the worker supervisor and start it
  let worker_supervisor_result =
    workers
    |> list.index_fold(worker_supervisor, fn(worker_supervisor, actor, idx) {
      sup.add(
        worker_supervisor,
        sup.worker_child("worker_" <> int.to_string(idx), fn() {
          process.subject_owner(actor) |> Ok
        })
          |> sup.restart(sup.Transient),
      )
    })
    |> sup.start_link()
    |> result.map_error(WorkerSupervisorStartError)

  use worker_supervisor <- result.try(worker_supervisor_result)

  // Add the pool and worker supervisors to the main supervisor
  let main_supervisor_result =
    sup.add(
      main_supervisor,
      sup.worker_child("pool", fn() {
        process.subject_owner(pool_subject) |> Ok
      })
        |> sup.restart(sup.Transient),
    )
    |> sup.add(
      sup.supervisor_child("worker_supervisor", fn() { Ok(worker_supervisor) })
      |> sup.restart(sup.Transient),
    )
    |> sup.start_link()
    |> result.map_error(PoolSupervisorStartError)

  use main_supervisor <- result.try(main_supervisor_result)

  Ok(Pool(subject: pool_subject, supervisor: main_supervisor))
}

fn check_out(
  pool: Pool(resource_type),
  caller: Subject(UsageResult),
  timeout: Int,
) -> Result(WorkerSubject(resource_type), ApplyError) {
  process.try_call(pool.subject, CheckOut(_, caller:), timeout)
  |> result.replace_error(CheckOutTimeout)
  |> result.flatten
}

fn check_in(
  pool: Pool(resource_type),
  worker_subject: WorkerSubject(resource_type),
) {
  process.send(pool.subject, CheckIn(worker_subject))
}

pub fn apply(
  pool: Pool(resource_type),
  timeout: Int,
  next: fn(resource_type) -> result_type,
) -> Result(result_type, ApplyError) {
  let self = process.new_subject()
  use worker_subject <- result.try(check_out(pool, self, timeout))

  // Use manual send instead of try_call so we can use the same caller subject
  // we sent with the check out message
  process.send(
    worker_subject,
    UseResource(self, unsafe_coerce_to_dynamic_function(next)),
  )

  let usage_result =
    process.receive(self, timeout)
    |> result.map(fn(result) {
      result
      |> result.map(unsafe_coerce_to_return_type)
      |> result.map_error(WorkerCrashed)
    })

  let usage_result = case usage_result {
    // Timeout
    Error(Nil) -> Error(WorkerCallTimeout)
    Ok(Error(err)) -> Error(err)
    Ok(Ok(result)) -> Ok(result)
  }

  check_in(pool, worker_subject)

  usage_result
}

// ----- Pool ----- //

pub opaque type Pool(resource_type) {
  Pool(subject: Subject(PoolMsg(resource_type)), supervisor: Pid)
}

type PoolState(resource_type) {
  PoolState(
    workers: deque.Deque(Worker(resource_type)),
    strategy: Strategy,
    live_workers: LiveWorkers(resource_type),
    selector: process.Selector(PoolMsg(resource_type)),
  )
}

type LiveWorkers(resource_type) =
  Dict(Pid, LiveWorker(resource_type))

type LiveWorker(resource_type) {
  LiveWorker(worker: Worker(resource_type), caller: Subject(UsageResult))
}

type PoolMsg(resource_type) {
  CheckIn(WorkerSubject(resource_type))
  CheckOut(
    reply_to: Subject(Result(WorkerSubject(resource_type), ApplyError)),
    caller: Subject(UsageResult),
  )
  PoolExit(process.ExitMessage)
  WorkerDown(process.ProcessDown)
}

fn handle_pool_message(
  msg: PoolMsg(resource_type),
  state: PoolState(resource_type),
) {
  // TODO: process monitoring
  case msg {
    CheckIn(worker_subject) -> {
      // If the checked-in process is a currently live worker, remove it from
      // the live_workers dict
      let live_workers =
        dict.delete(state.live_workers, worker_subject |> process.subject_owner)

      // Monitor the new worker process
      let monitor =
        process.monitor_process(worker_subject |> process.subject_owner)

      let new_workers =
        deque.push_back(
          state.workers,
          Worker(subject: worker_subject, monitor:),
        )

      let selector =
        state.selector
        |> process.selecting_process_down(monitor, WorkerDown)

      actor.continue(
        PoolState(..state, workers: new_workers, live_workers:, selector:),
      )
    }
    CheckOut(reply_to:, caller:) -> {
      // We always push to the back, so for FIFO, we pop front,
      // and for LIFO, we pop back
      let get_result = case state.strategy {
        FIFO -> deque.pop_front(state.workers)
        LIFO -> deque.pop_back(state.workers)
      }

      case get_result {
        Ok(#(worker, new_workers)) -> {
          let live_workers =
            dict.insert(
              state.live_workers,
              worker.subject |> process.subject_owner,
              LiveWorker(worker:, caller:),
            )
          actor.send(reply_to, Ok(worker.subject))
          actor.continue(PoolState(..state, workers: new_workers))
        }
        Error(_) -> {
          actor.send(reply_to, Error(NoResourcesAvailable))
          actor.continue(state)
        }
      }
    }
    PoolExit(exit_message) -> {
      io.debug("Pool exited")
      // TODO: cleanup
      actor.Stop(process.Normal)
    }
    WorkerDown(process_down) -> {
      let #(maybe_worker, new_workers) =
        state.workers
        |> deque.to_list
        |> list.partition(fn(worker) {
          process.subject_owner(worker.subject) == process_down.pid
        })

      // If the worker exists, demonitor it
      let selector = case maybe_worker {
        [worker] -> {
          process.demonitor_process(worker.monitor)

          state.selector
          |> process.deselecting_process_down(worker.monitor)
        }
        _ -> state.selector
      }

      // If the process was a live worker, send an error message back
      // to the caller
      case dict.get(state.live_workers, process_down.pid) {
        Ok(live_worker) -> {
          process.send(live_worker.caller, Error(process_down))
        }
        Error(_) -> Nil
      }

      // Delete the process from the live_workers dict
      let live_workers = dict.delete(state.live_workers, process_down.pid)

      actor.continue(
        PoolState(
          ..state,
          live_workers:,
          selector:,
          workers: new_workers |> deque.from_list,
        ),
      )
    }
  }
}

fn pool_spec(
  pool_config: PoolConfig(resource_type, resource_create_error),
  init_timeout: Int,
) -> actor.Spec(PoolState(resource_type), PoolMsg(resource_type)) {
  actor.Spec(init_timeout:, loop: handle_pool_message, init: fn() {
    let self = process.new_subject()

    let selector =
      process.new_selector()
      |> process.selecting(self, function.identity)
      |> process.selecting_trapped_exits(PoolExit)

    let state =
      PoolState(
        workers: deque.new(),
        strategy: pool_config.strategy,
        live_workers: dict.new(),
        selector:,
      )

    // Trap exits
    // process.trap_exits(True)

    actor.Ready(state, selector)
  })
}

// ----- Worker ---- //

type Worker(resource_type) {
  Worker(subject: WorkerSubject(resource_type), monitor: process.ProcessMonitor)
}

type WorkerSubject(resource_type) =
  Subject(WorkerMsg(resource_type))

type WorkerMsg(resource_type) {
  UseResource(
    reply_to: Subject(UsageResult),
    function: fn(resource_type) -> dynamic.Dynamic,
  )
  WorkerExit(
    process.ExitMessage,
    resource_shutdown_function: fn(resource_type) -> Nil,
  )
}

type UsageResult =
  Result(dynamic.Dynamic, process.ProcessDown)

fn worker_spec(
  pool_subject: Subject(PoolMsg(resource_type)),
  resource_create_function: fn() -> Result(resource_type, resource_create_error),
  resource_shutdown_function: fn(resource_type) -> Nil,
  init_timeout: Int,
) -> actor.Spec(resource_type, WorkerMsg(resource_type)) {
  actor.Spec(init_timeout:, loop: handle_worker_message, init: fn() {
    case resource_create_function() {
      Ok(resource) -> {
        // Check in the worker
        let self = process.new_subject()
        process.send(pool_subject, CheckIn(self))
        process.trap_exits(True)

        let selector =
          process.new_selector()
          |> process.selecting(self, function.identity)
          |> process.selecting_trapped_exits(WorkerExit(
            _,
            resource_shutdown_function,
          ))

        actor.Ready(resource, selector)
      }
      Error(resource_create_error) -> {
        actor.Failed(resource_create_error |> string.inspect)
      }
    }
  })
}

fn handle_worker_message(msg: WorkerMsg(resource_type), resource: resource_type) {
  case msg {
    UseResource(reply_to:, function:) -> {
      actor.send(reply_to, Ok(function(resource)))
      actor.continue(resource)
    }
    WorkerExit(exit_message, resource_shutdown_function:) -> {
      resource_shutdown_function(resource)
      actor.Stop(exit_message.reason)
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
