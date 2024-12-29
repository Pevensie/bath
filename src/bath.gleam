import gleam/deque
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/function
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/result

// ---- Pool config ----- //

pub type CheckoutStrategy {
  FIFO
  LIFO
}

pub type CreationStrategy {
  Lazy
  Eager
}

pub opaque type PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: Int,
    create_resource: fn() -> Result(resource_type, resource_create_error),
    shutdown_resource: fn(resource_type) -> Nil,
    checkout_strategy: CheckoutStrategy,
    creation_strategy: CreationStrategy,
  )
}

pub fn new(
  resource create_resource: fn() -> Result(resource_type, resource_create_error),
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: 10,
    create_resource: create_resource,
    shutdown_resource: fn(_) { Nil },
    checkout_strategy: FIFO,
    creation_strategy: Lazy,
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

pub fn with_checkout_strategy(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  strategy checkout_strategy: CheckoutStrategy,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, checkout_strategy:)
}

pub fn with_creation_strategy(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  strategy creation_strategy: CreationStrategy,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, creation_strategy:)
}

// ----- Lifecycle functions ---- //

pub type StartError(resource_create_error) {
  PoolStartResourceCreateError(resource_create_error)
  ActorStartError(actor.StartError)
}

pub type ApplyError(resource_create_error) {
  NoResourcesAvailable
  CheckOutResourceCreateError(resource_create_error)
  CheckOutTimeout
}

pub fn child_spec(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  timeout init_timeout: Int,
) -> Result(
  actor.Spec(
    State(resource_type, resource_create_error),
    Msg(resource_type, resource_create_error),
  ),
  StartError(resource_create_error),
) {
  let #(resources_result, current_size) = case pool_config.creation_strategy {
    Lazy -> #(Ok(deque.new()), 0)
    Eager -> #(
      list.repeat("", pool_config.size)
        |> list.try_map(fn(_) { pool_config.create_resource() })
        |> result.map(deque.from_list)
        |> result.map_error(PoolStartResourceCreateError),
      pool_config.size,
    )
  }

  use resources <- result.try(resources_result)

  Ok(pool_spec(pool_config, resources, current_size, init_timeout))
}

pub fn start(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  timeout init_timeout: Int,
) -> Result(
  Pool(resource_type, resource_create_error),
  StartError(resource_create_error),
) {
  use spec <- result.try(child_spec(pool_config, init_timeout))

  actor.start_spec(spec)
  |> result.map(fn(subject) { Pool(subject:) })
  |> result.map_error(ActorStartError)
}

/// Checks out a resource from the pool, sending the caller Pid for the pool to
/// monitor in case the client dies. This allows the pool to create a new resource
/// later if required.
fn check_out(
  pool: Pool(resource_type, resource_create_error),
  caller: Pid,
  timeout: Int,
) -> Result(resource_type, ApplyError(resource_create_error)) {
  process.try_call(pool.subject, CheckOut(_, caller:), timeout)
  |> result.replace_error(CheckOutTimeout)
  |> result.flatten
}

fn check_in(
  pool: Pool(resource_type, resource_create_error),
  resource: resource_type,
  caller: Pid,
) {
  process.send(pool.subject, CheckIn(resource:, caller:))
}

pub fn apply(
  pool: Pool(resource_type, resource_create_error),
  timeout: Int,
  next: fn(resource_type) -> result_type,
) -> Result(result_type, ApplyError(resource_create_error)) {
  let self = process.self()
  use resource <- result.try(check_out(pool, self, timeout))

  let usage_result = next(resource)

  check_in(pool, resource, self)

  Ok(usage_result)
}

pub fn shutdown(pool: Pool(resource_type, resource_create_error)) {
  process.send_exit(pool.subject |> process.subject_owner)
}

// ----- Pool ----- //

pub opaque type Pool(resource_type, resource_create_error) {
  Pool(subject: Subject(Msg(resource_type, resource_create_error)))
}

pub opaque type State(resource_type, resource_create_error) {
  State(
    // Config
    checkout_strategy: CheckoutStrategy,
    creation_strategy: CreationStrategy,
    max_size: Int,
    create_resource: fn() -> Result(resource_type, resource_create_error),
    shutdown_resource: fn(resource_type) -> Nil,
    // State
    resources: deque.Deque(resource_type),
    current_size: Int,
    live_resources: LiveResources(resource_type),
    selector: process.Selector(Msg(resource_type, resource_create_error)),
  )
}

type LiveResources(resource_type) =
  Dict(Pid, LiveResource(resource_type))

type LiveResource(resource_type) {
  LiveResource(resource: resource_type, monitor: process.ProcessMonitor)
}

pub opaque type Msg(resource_type, resource_create_error) {
  CheckIn(resource: resource_type, caller: Pid)
  CheckOut(
    reply_to: Subject(Result(resource_type, ApplyError(resource_create_error))),
    caller: Pid,
  )
  PoolExit(process.ExitMessage)
  CallerDown(process.ProcessDown)
}

fn handle_pool_message(
  msg: Msg(resource_type, resource_create_error),
  state: State(resource_type, resource_create_error),
) {
  io.debug(state.live_resources)
  case msg {
    CheckIn(resource:, caller:) -> {
      // If the checked-in process currently has a live resource, remove it from
      // the live_resources dict
      let caller_live_resource = dict.get(state.live_resources, caller)
      let live_resources = dict.delete(state.live_resources, caller)

      let selector = case caller_live_resource {
        Ok(live_resource) -> {
          process.demonitor_process(live_resource.monitor)

          state.selector
          |> process.deselecting_process_down(live_resource.monitor)
        }
        Error(_) -> state.selector
      }

      let new_resources = deque.push_back(state.resources, resource)

      actor.continue(
        State(..state, resources: new_resources, live_resources:, selector:),
      )
    }
    CheckOut(reply_to:, caller:) -> {
      // We always push to the back, so for FIFO, we pop front,
      // and for LIFO, we pop back
      let get_result = case state.checkout_strategy {
        FIFO -> deque.pop_front(state.resources)
        LIFO -> deque.pop_back(state.resources)
      }

      // Try to get a new resource, either from the pool or by creating a new one
      // if we still have capacity
      let resource_result = case get_result {
        // Use an existing resource - current size hasn't changed
        Ok(#(resource, new_resources)) ->
          Ok(#(resource, new_resources, state.current_size))
        Error(_) -> {
          // Nothing in the pool. Create a new resource if we can
          case state.current_size < state.max_size {
            True -> {
              use resource <- result.try(
                state.create_resource()
                |> result.map_error(CheckOutResourceCreateError),
              )
              // Checked-in resources queue hasn't changed, but we've added a new resource
              // so current size has increased
              Ok(#(resource, state.resources, state.current_size + 1))
            }
            False -> Error(NoResourcesAvailable)
          }
        }
      }

      case resource_result {
        Error(err) -> {
          // Nothing has changed
          actor.send(reply_to, Error(err))
          actor.continue(state)
        }
        Ok(#(resource, new_resources, new_current_size)) -> {
          // Monitor the caller process
          let monitor = process.monitor_process(caller)
          let selector =
            state.selector
            |> process.selecting_process_down(monitor, CallerDown)

          let live_resources =
            dict.insert(
              state.live_resources,
              caller,
              LiveResource(resource:, monitor:),
            )

          actor.send(reply_to, Ok(resource))
          actor.continue(
            State(
              ..state,
              resources: new_resources,
              current_size: new_current_size,
              selector:,
              live_resources:,
            ),
          )
        }
      }
    }
    PoolExit(exit_message) -> {
      // Don't clean up live resources, as they may be in use
      state.resources
      |> deque.to_list
      |> list.each(state.shutdown_resource)

      actor.Stop(exit_message.reason)
    }
    CallerDown(process_down) -> {
      io.debug("Caller down")
      // If the caller was a live resource, either create a new one or
      // decrement the current size depending on the creation strategy
      case dict.get(state.live_resources, process_down.pid) {
        // Continue as normal, ignoring this message
        Error(_) -> actor.continue(state)
        Ok(live_resource) -> {
          // Demonitor the process
          process.demonitor_process(live_resource.monitor)
          let selector =
            state.selector
            |> process.deselecting_process_down(live_resource.monitor)

          let #(new_resources, new_current_size) = case
            state.creation_strategy
          {
            // If we create lazily, just decrement the current size - a new resource
            // will be created when required
            Lazy -> #(state.resources, state.current_size - 1)
            // Otherwise, create a new resource, warning if resource creation fails
            Eager -> {
              case state.create_resource() {
                // Size hasn't changed
                Ok(resource) -> #(
                  deque.push_back(state.resources, resource),
                  state.current_size,
                )
                // Size has changed
                Error(resource_create_error) -> {
                  io.debug("Bath: Resource creation failed")
                  io.debug(resource_create_error)
                  #(state.resources, state.current_size)
                }
              }
            }
          }

          actor.continue(
            State(
              ..state,
              resources: new_resources,
              current_size: new_current_size,
              selector:,
              live_resources: dict.delete(
                state.live_resources,
                process_down.pid,
              ),
            ),
          )
        }
      }
    }
  }
}

fn pool_spec(
  pool_config: PoolConfig(resource_type, resource_create_error),
  resources: deque.Deque(resource_type),
  current_size: Int,
  init_timeout: Int,
) -> actor.Spec(
  State(resource_type, resource_create_error),
  Msg(resource_type, resource_create_error),
) {
  actor.Spec(init_timeout:, loop: handle_pool_message, init: fn() {
    let self = process.new_subject()

    // Trap exits
    process.trap_exits(True)

    let selector =
      process.new_selector()
      |> process.selecting(self, function.identity)
      |> process.selecting_trapped_exits(PoolExit)

    let state =
      State(
        resources:,
        checkout_strategy: pool_config.checkout_strategy,
        creation_strategy: pool_config.creation_strategy,
        live_resources: dict.new(),
        selector:,
        current_size:,
        max_size: pool_config.size,
        create_resource: pool_config.create_resource,
        shutdown_resource: pool_config.shutdown_resource,
      )

    actor.Ready(state, selector)
  })
}
