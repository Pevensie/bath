import gleam/deque
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/otp/actor
import gleam/otp/supervision
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
pub opaque type Builder(resource_type) {
  Builder(
    size: Int,
    create_resource: fn() -> Result(resource_type, String),
    shutdown_resource: fn(resource_type) -> Nil,
    checkout_strategy: CheckoutStrategy,
    creation_strategy: CreationStrategy,
    log_errors: Bool,
  )
}

/// Create a new [`Builder`](#Builder) for creating a pool of resources.
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
/// | `log_errors` | `False` |
pub fn new(
  resource create_resource: fn() -> Result(resource_type, String),
) -> Builder(resource_type) {
  Builder(
    size: 10,
    create_resource:,
    shutdown_resource: fn(_) { Nil },
    checkout_strategy: FIFO,
    creation_strategy: Lazy,
    log_errors: False,
  )
}

/// Set the pool size. Defaults to 10.
pub fn size(
  builder builder: Builder(resource_type),
  size size: Int,
) -> Builder(resource_type) {
  Builder(..builder, size:)
}

/// Set a shutdown function to be run for each resource when the pool exits.
pub fn on_shutdown(
  builder builder: Builder(resource_type),
  shutdown shutdown_resource: fn(resource_type) -> Nil,
) -> Builder(resource_type) {
  Builder(..builder, shutdown_resource:)
}

/// Change the checkout strategy for the pool. Defaults to `FIFO`.
pub fn checkout_strategy(
  builder builder: Builder(resource_type),
  strategy checkout_strategy: CheckoutStrategy,
) -> Builder(resource_type) {
  Builder(..builder, checkout_strategy:)
}

/// Change the resource creation strategy for the pool. Defaults to `Lazy`.
pub fn creation_strategy(
  builder builder: Builder(resource_type),
  strategy creation_strategy: CreationStrategy,
) -> Builder(resource_type) {
  Builder(..builder, creation_strategy:)
}

/// Set whether the pool logs errors when resources fail to create.
pub fn log_errors(
  builder builder: Builder(resource_type),
  log_errors log_errors: Bool,
) -> Builder(resource_type) {
  Builder(..builder, log_errors:)
}

// ----- Lifecycle functions ---- //

/// An error returned when failing to apply a function to a pooled resource.
pub type ApplyError {
  NoResourcesAvailable
  CheckOutResourceCreateError(error: String)
}

/// An error returned when the resource pool fails to shut down.
pub type ShutdownError {
  /// There are still resources checked out. Ignore this failure case by
  /// calling [`shutdown`](#shutdown) function with `force` set to `True`.
  ResourcesInUse
}

/// Return the [`ChildSpecification`](https://hexdocs.pm/gleam_otp/gleam/otp/supervision.html#ChildSpecification)
/// for creating a supervised resource pool.
///
/// You must provide a selector to receive the [`Pool`](#Pool) value representing the
/// pool once it has started.
///
/// ## Example
///
/// ```gleam
/// import bath
/// import gleam/erlang/process
/// import gleam/otp/static_supervisor as supervisor
///
/// fn main() {
///   let pool_receiver = process.new_subject()
///
///   let assert Ok(_started) =
///     supervisor.new(supervisor.OneForOne)
///     |> supervisor.add(
///       bath.new(create_resource)
///       |> bath.supervised(pool_receiver, 1000)
///     )
///     |> supervisor.start
///
///   let assert Ok(pool) =
///     process.receive(pool_receiver)
///
///   let assert Ok(_) = bath.apply(pool, fn(res) { echo res })
/// }
/// ```
pub fn supervised(
  builder builder: Builder(resource_type),
  receiver pool_receiver: Subject(Pool(resource_type)),
  timeout init_timeout: Int,
) {
  supervision.worker(fn() {
    use started <- result.try(
      actor_builder(builder, init_timeout)
      |> actor.start,
    )

    process.send(pool_receiver, Pool(started.data))
    Ok(started)
  })
}

/// Start an unsupervised pool using the given [`Builder`](#Builder) and return a
/// [`Pool`](#Pool). In most cases, you should use the [`supervised`](#supervised)
/// function instead.
pub fn start(
  builder builder: Builder(resource_type),
  timeout init_timeout: Int,
) -> Result(Pool(resource_type), actor.StartError) {
  use started <- result.try(
    actor_builder(builder, init_timeout)
    |> actor.start,
  )

  Ok(Pool(started.data))
}

/// Checks out a resource from the pool, sending the caller Pid for the pool to
/// monitor in case the client dies. This allows the pool to create a new resource
/// later if required.
fn check_out(
  pool: Pool(resource_type),
  caller: Pid,
  timeout: Int,
) -> Result(resource_type, ApplyError) {
  process.call(pool.subject, timeout, CheckOut(_, caller:))
}

fn check_in(pool: Pool(resource_type), resource: resource_type, caller: Pid) {
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
  pool pool: Pool(resource_type),
  timeout timeout: Int,
  next next: fn(resource_type) -> result_type,
) -> Result(result_type, ApplyError) {
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
  pool pool: Pool(resource_type),
  force force: Bool,
  timeout timeout: Int,
) -> Result(Nil, ShutdownError) {
  process.call(pool.subject, timeout, Shutdown(_, force))
}

// ----- Pool ----- //

/// The interface for interacting with a pool of resources in Bath.
pub opaque type Pool(resource_type) {
  Pool(subject: Subject(Msg(resource_type)))
}

/// Pool actor state.
pub opaque type State(resource_type) {
  State(
    // Config
    checkout_strategy: CheckoutStrategy,
    creation_strategy: CreationStrategy,
    max_size: Int,
    create_resource: fn() -> Result(resource_type, String),
    shutdown_resource: fn(resource_type) -> Nil,
    // State
    resources: deque.Deque(resource_type),
    current_size: Int,
    live_resources: LiveResources(resource_type),
    selector: process.Selector(Msg(resource_type)),
    log_errors: Bool,
  )
}

type LiveResources(resource_type) =
  Dict(Pid, LiveResource(resource_type))

type LiveResource(resource_type) {
  LiveResource(resource: resource_type, monitor: process.Monitor)
}

/// A message sent to the pool actor.
pub opaque type Msg(resource_type) {
  CheckIn(resource: resource_type, caller: Pid)
  CheckOut(reply_to: Subject(Result(resource_type, ApplyError)), caller: Pid)
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  Shutdown(reply_to: process.Subject(Result(Nil, ShutdownError)), force: Bool)
}

fn handle_pool_message(state: State(resource_type), msg: Msg(resource_type)) {
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
                  log_resource_creation_error(state.log_errors, err)
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

      case exit_message.reason {
        process.Abnormal(reason) ->
          string.inspect(reason)
          |> actor.stop_abnormal
        process.Killed -> actor.stop_abnormal("Killed")
        process.Normal -> actor.stop()
      }
    }
    Shutdown(reply_to:, force:) -> {
      case dict.size(state.live_resources), force {
        // No live resource, shut down
        0, _ -> {
          state.resources
          |> deque.to_list
          |> list.each(state.shutdown_resource)

          actor.send(reply_to, Ok(Nil))
          actor.stop()
        }
        _, True -> {
          // Force shutdown
          actor.send(reply_to, Ok(Nil))
          actor.stop()
        }
        _, False -> {
          actor.send(reply_to, Error(ResourcesInUse))
          actor.continue(state)
        }
      }
    }
    CallerDown(process_down) -> {
      // We don't monitor ports
      let assert process.ProcessDown(pid: process_down_pid, ..) = process_down

      // If the caller was a live resource, either create a new one or
      // decrement the current size depending on the creation strategy
      case dict.get(state.live_resources, process_down_pid) {
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
                  log_resource_creation_error(
                    state.log_errors,
                    resource_create_error,
                  )
                  #(state.resources, state.current_size)
                }
              }
            }
          }

          State(
            ..state,
            resources: new_resources,
            current_size: new_current_size,
            selector:,
            live_resources: dict.delete(state.live_resources, process_down_pid),
          )
          |> actor.continue
          |> actor.with_selector(selector)
        }
      }
    }
  }
}

/// Create the resources for a pool, returning a deque of resources and the number of
/// resources created.
fn create_pool_resources(
  builder: Builder(resource_type),
) -> Result(#(deque.Deque(resource_type), Int), String) {
  case builder.creation_strategy {
    Lazy -> Ok(#(deque.new(), 0))
    Eager -> {
      let create_result =
        list.repeat("", builder.size)
        |> try_map_returning(fn(_) { builder.create_resource() })
        |> result.map(deque.from_list)

      case create_result {
        Ok(resources) -> Ok(#(resources, builder.size))
        Error(#(created_resources, error)) -> {
          created_resources
          |> list.each(builder.shutdown_resource)

          Error(error)
        }
      }
    }
  }
}

fn actor_builder(
  builder: Builder(resource_type),
  init_timeout: Int,
) -> actor.Builder(
  State(resource_type),
  Msg(resource_type),
  Subject(Msg(resource_type)),
) {
  actor.new_with_initialiser(init_timeout, fn(self) {
    use #(resources, current_size) <- result.try(create_pool_resources(builder))

    // Trap exits
    process.trap_exits(True)

    let selector =
      process.new_selector()
      |> process.select(self)
      |> process.select_trapped_exits(PoolExit)

    let state =
      State(
        resources:,
        checkout_strategy: builder.checkout_strategy,
        creation_strategy: builder.creation_strategy,
        live_resources: dict.new(),
        selector:,
        current_size:,
        max_size: builder.size,
        create_resource: builder.create_resource,
        shutdown_resource: builder.shutdown_resource,
        log_errors: builder.log_errors,
      )

    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(self)
    |> Ok
  })
  |> actor.on_message(handle_pool_message)
}

// ----- Utils ----- //

fn monitor_process(selector: process.Selector(Msg(resource_type)), pid: Pid) {
  let monitor = process.monitor(pid)
  let selector =
    selector
    |> process.select_specific_monitor(monitor, CallerDown)
  #(monitor, selector)
}

fn demonitor_process(
  selector: process.Selector(Msg(resource_type)),
  monitor: process.Monitor,
) {
  let selector =
    selector
    |> process.deselect_specific_monitor(monitor)
  selector
}

fn log_resource_creation_error(
  log_errors: Bool,
  resource_create_error: String,
) -> Nil {
  case log_errors {
    True ->
      logging.log(
        logging.Error,
        "Bath: Resource creation failed: " <> resource_create_error,
      )
    False -> Nil
  }
}

/// Iterate over a list, applying a function that returns a result.
/// If an `Error` is returned by the function, iteration stops and all
/// items to this point are returned.
@internal
pub fn try_map_returning(
  over list: List(a),
  with fun: fn(a) -> Result(b, e),
) -> Result(List(b), #(List(b), e)) {
  list
  |> list.try_fold([], fn(acc, item) {
    case fun(item) {
      Ok(value) -> Ok([value, ..acc])
      Error(error) -> Error(#(acc, error))
    }
  })
  |> result.map(list.reverse)
}
