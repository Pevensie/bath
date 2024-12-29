import gleam/deque
import gleam/dict.{type Dict}
import gleam/dynamic
import gleam/erlang/process.{type Pid, type Subject}
import gleam/function
import gleam/list
import gleam/otp/actor
import gleam/result
import gleam/string
import logging

// ---- Pool config ----- //

/// The strategy used to check out a resource from the pool.
pub type CheckoutStrategy {
  FIFO
  LIFO
}

/// How to create resources in the pool. `Lazy` will create resources when
/// required (i.e. the pool is empty but has extra capacity), while `Eager` will
/// create the maximum number of resources upfront.
pub type CreationStrategy {
  Lazy
  Eager
}

/// Configuration for a [`Pool`](#Pool).
pub opaque type PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: Int,
    create_resource: fn() -> Result(resource_type, resource_create_error),
    shutdown_resource: fn(resource_type) -> Nil,
    checkout_strategy: CheckoutStrategy,
    creation_strategy: CreationStrategy,
  )
}

/// Create a new [`PoolConfig`](#PoolConfig) for creating a pool of resources.
///
/// ```gleam
/// import bath
/// import fake_db
///
/// pub fn main() {
///   // Create a pool of 10 connections to some fictional database.
///   let assert Ok(pool) =
///     bath.new(fn() { fake_db.connect() })
///     |> bath.with_size(10)
///     |> bath.start(1000)
/// }
/// ```
///
/// ### Default values
///
/// | Config | Default |
/// |--------|---------|
/// | `size`   | 10      |
/// | `shutdown_resource` | `fn(_resource) { Nil }` |
/// | `checkout_strategy` | `FIFO` |
/// | `creation_strategy` | `Lazy` |
pub fn new(
  resource create_resource: fn() -> Result(resource_type, resource_create_error),
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(
    size: 10,
    create_resource:,
    shutdown_resource: fn(_) { Nil },
    checkout_strategy: FIFO,
    creation_strategy: Lazy,
  )
}

/// Set the pool size. Defaults to 10.
pub fn with_size(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  size size: Int,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, size:)
}

/// Set a shutdown function to be run for each resource when the pool exits.
pub fn with_shutdown(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  shutdown shutdown_resource: fn(resource_type) -> Nil,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, shutdown_resource:)
}

/// Change the checkout strategy for the pool. Defaults to `FIFO`.
pub fn with_checkout_strategy(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  strategy checkout_strategy: CheckoutStrategy,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, checkout_strategy:)
}

/// Change the resource creation strategy for the pool. Defaults to `Lazy`.
pub fn with_creation_strategy(
  config pool_config: PoolConfig(resource_type, resource_create_error),
  strategy creation_strategy: CreationStrategy,
) -> PoolConfig(resource_type, resource_create_error) {
  PoolConfig(..pool_config, creation_strategy:)
}

// ----- Lifecycle functions ---- //

/// An error returned when creating a [`Pool`](#Pool).
pub type StartError(resource_create_error) {
  PoolStartResourceCreateError(resource_create_error)
  ActorStartError(actor.StartError)
}

/// An error returned when failing to apply a function to a pooled resource.
pub type ApplyError(resource_create_error) {
  NoResourcesAvailable
  CheckOutResourceCreateError(resource_create_error)
  CheckOutTimeout
}

/// An error returned when the resource pool fails to shut down.
pub type ShutdownError {
  /// There are still resources checked out. Ignore this failure case by
  /// calling [`shutdown`](#shutdown) function with `force` set to `True`.
  ResourcesInUse
  /// The shutdown timeout expired.
  ShutdownTimeout
  /// The pool was already down or failed to send the response message.
  CalleeDown(reason: dynamic.Dynamic)
}

/// Create a child actor spec pool actor, for use in your application's supervision tree,
/// using the given [`PoolConfig`](#PoolConfig). Once the pool is started, use
/// [`from_subject`](#from_subject) to create a [`Pool`](#Pool) from the
/// `Subject(bath.Msg(a, b))`.
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

/// Create a [`Pool`](#Pool) from a `Subject(bath.Msg(a, b))`. Useful when
/// creating a pool as part of a supervision tree via the
/// [`child_spec`](#child_spec) function.
pub fn from_subject(
  subject subject: Subject(Msg(resource_type, resource_create_error)),
) -> Pool(resource_type, resource_create_error) {
  Pool(subject:)
}

/// Start a  pool actor using the given [`PoolConfig`](#PoolConfig) and return a
/// [`Pool`](#Pool).
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

/// Check out a resource from the pool, apply the `next` function, then check
/// the resource back in.
///
/// ```gleam
/// let assert Ok(pool) =
///   bath.new(fn() { Ok("Some pooled resource") })
///   |> bath.start(1000)
///
/// use resource <- bath.apply(pool, 1000)
/// // Do stuff with resource...
/// ```
pub fn apply(
  pool pool: Pool(resource_type, resource_create_error),
  timeout timeout: Int,
  next next: fn(resource_type) -> result_type,
) -> Result(result_type, ApplyError(resource_create_error)) {
  let self = process.self()
  use resource <- result.try(check_out(pool, self, timeout))

  let usage_result = next(resource)

  check_in(pool, resource, self)

  Ok(usage_result)
}

/// Shut down the pool, calling the `shutdown_function` on each
/// resource in the pool. Calling with `force` set to `True` will
/// force the shutdown, not calling the `shutdown_function` on any
/// resources.
///
/// Will fail if there are still resources checked out, unless `force` is
/// `True`.
pub fn shutdown(
  pool pool: Pool(resource_type, resource_create_error),
  force force: Bool,
  timeout timeout: Int,
) {
  process.try_call(pool.subject, Shutdown(_, force), timeout)
  |> result.map_error(fn(err) {
    case err {
      process.CallTimeout -> ShutdownTimeout
      process.CalleeDown(reason) -> CalleeDown(reason:)
    }
  })
  |> result.flatten
}

// ----- Pool ----- //

/// The interface for interacting with a pool of resources in Bath.
pub opaque type Pool(resource_type, resource_create_error) {
  Pool(subject: Subject(Msg(resource_type, resource_create_error)))
}

/// Pool actor state.
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

/// A message sent to the pool actor.
pub opaque type Msg(resource_type, resource_create_error) {
  CheckIn(resource: resource_type, caller: Pid)
  CheckOut(
    reply_to: Subject(Result(resource_type, ApplyError(resource_create_error))),
    caller: Pid,
  )
  PoolExit(process.ExitMessage)
  CallerDown(process.ProcessDown)
  Shutdown(reply_to: process.Subject(Result(Nil, ShutdownError)), force: Bool)
}

fn handle_pool_message(
  msg: Msg(resource_type, resource_create_error),
  state: State(resource_type, resource_create_error),
) {
  case msg {
    CheckIn(resource:, caller:) -> {
      // If the checked-in process currently has a live resource, remove it from
      // the live_resources dict
      let caller_live_resource = dict.get(state.live_resources, caller)
      let live_resources = dict.delete(state.live_resources, caller)

      let selector = case caller_live_resource {
        Ok(live_resource) -> {
          demonitor_process(state.selector, live_resource.monitor)
        }
        Error(_) -> state.selector
      }

      let new_resources = deque.push_back(state.resources, resource)

      actor.with_selector(
        actor.continue(
          State(..state, resources: new_resources, live_resources:, selector:),
        ),
        selector,
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
                |> result.map_error(fn(err) {
                  log_resource_creation_error(err)
                  CheckOutResourceCreateError(err)
                }),
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
          let #(monitor, selector) = monitor_process(state.selector, caller)

          let live_resources =
            dict.insert(
              state.live_resources,
              caller,
              LiveResource(resource:, monitor:),
            )

          actor.send(reply_to, Ok(resource))
          actor.with_selector(
            actor.continue(
              State(
                ..state,
                resources: new_resources,
                current_size: new_current_size,
                selector:,
                live_resources:,
              ),
            ),
            selector,
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
    Shutdown(reply_to:, force:) -> {
      case dict.size(state.live_resources), force {
        // No live resource, shut down
        0, _ -> {
          state.resources
          |> deque.to_list
          |> list.each(state.shutdown_resource)

          actor.send(reply_to, Ok(Nil))
          actor.Stop(process.Normal)
        }
        _, True -> {
          // Force shutdown
          actor.send(reply_to, Ok(Nil))
          actor.Stop(process.Normal)
        }
        _, False -> {
          actor.send(reply_to, Error(ResourcesInUse))
          actor.continue(state)
        }
      }
    }
    CallerDown(process_down) -> {
      // If the caller was a live resource, either create a new one or
      // decrement the current size depending on the creation strategy
      case dict.get(state.live_resources, process_down.pid) {
        // Continue as normal, ignoring this message
        Error(_) -> actor.continue(state)
        Ok(live_resource) -> {
          // Demonitor the process
          let selector =
            demonitor_process(state.selector, live_resource.monitor)

          // Shutdown the old resource
          state.shutdown_resource(live_resource.resource)

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
                  log_resource_creation_error(resource_create_error)
                  #(state.resources, state.current_size)
                }
              }
            }
          }

          actor.with_selector(
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
            ),
            selector,
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

// ----- Utils ----- //

fn monitor_process(
  selector: process.Selector(Msg(resource_type, resource_create_error)),
  pid: Pid,
) {
  let monitor = process.monitor_process(pid)
  let selector =
    selector
    |> process.selecting_process_down(monitor, CallerDown)
  #(monitor, selector)
}

fn demonitor_process(
  selector: process.Selector(Msg(resource_type, resource_create_error)),
  monitor: process.ProcessMonitor,
) {
  let selector =
    selector
    |> process.deselecting_process_down(monitor)
  selector
}

fn log_resource_creation_error(resource_create_error: resource_create_error) {
  logging.log(
    logging.Error,
    "Bath: Resource creation failed: " <> string.inspect(resource_create_error),
  )
}
