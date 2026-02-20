import gleam/deque
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/function
import gleam/int
import gleam/list
import gleam/option
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
    name: option.Option(process.Name(Msg(resource_type))),
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
///     |> bath.size(10)
///     |> bath.start(1000)
/// }
/// ```
///
/// ### Default values
///
/// | Config | Default |
/// |--------|---------|
/// | `name`   | `option.None` |
/// | `size`   | 10      |
/// | `shutdown_resource` | `fn(_resource) { Nil }` |
/// | `checkout_strategy` | `FIFO` |
/// | `creation_strategy` | `Lazy` |
/// | `log_errors` | `False` |
pub fn new(
  resource create_resource: fn() -> Result(resource_type, String),
) -> Builder(resource_type) {
  Builder(
    name: option.None,
    size: 10,
    create_resource:,
    shutdown_resource: fn(_) { Nil },
    checkout_strategy: FIFO,
    creation_strategy: Lazy,
    log_errors: False,
  )
}

/// Set the pool size. Defaults to 10. Will be clamped to a minimum of 1.
pub fn size(
  builder builder: Builder(resource_type),
  size size: Int,
) -> Builder(resource_type) {
  Builder(..builder, size: int.max(size, 1))
}

/// Set the name for the pool process. Defaults to `None`.
///
/// You will need to provide a name if you plan on using the pool under a static
/// supervisor.
pub fn name(
  builder builder: Builder(resource_type),
  name name: process.Name(Msg(resource_type)),
) -> Builder(resource_type) {
  Builder(..builder, name: option.Some(name))
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
  PoolShuttingDown
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
/// In order to use a supervised pool, your pool _must_ be named, otherwise you will
/// not be able to send messages to your pool. See the [`name`](#name) function.
///
/// ## Example
///
/// ```gleam
/// import bath
/// import gleam/erlang/process
/// import gleam/otp/static_supervisor as supervisor
///
/// fn main() {
///   // Create a name to interact with the pool once it's started under the
///   // static supervisor.
///   let pool_name = process.new_name("bath_pool")
///
///   let assert Ok(_started) =
///     supervisor.new(supervisor.OneForOne)
///     |> supervisor.add(
///       bath.new(create_resource)
///       |> bath.name(pool_name)
///       |> bath.supervised(1000)
///     )
///     |> supervisor.start
///
///   let pool = process.named_subject(pool_name)
///
///   // Do more stuff...
/// }
/// ```
pub fn supervised(
  builder builder: Builder(resource_type),
  timeout init_timeout: Int,
) {
  supervised_map(builder, function.identity, init_timeout)
}

/// Like [`supervised`](#supervised), but allows you to pass a mapping function to
/// transform the pool return value to the receiver. This is mostly useful for library
/// authors who wish to use Bath to create a pool of resources.
pub fn supervised_map(
  builder builder: Builder(resource_type),
  using mapper: fn(process.Subject(Msg(resource_type))) -> a,
  timeout init_timeout: Int,
) {
  supervision.worker(fn() {
    actor_builder(builder, mapper, init_timeout)
    |> actor.start
  })
}

/// Start an unsupervised pool using the given [`Builder`](#Builder) and return a
/// [`Pool`](#Pool). In most cases, you should use the [`supervised`](#supervised)
/// function instead.
pub fn start(
  builder builder: Builder(resource_type),
  timeout init_timeout: Int,
) -> Result(process.Subject(Msg(resource_type)), actor.StartError) {
  use started <- result.try(
    actor_builder(builder, function.identity, init_timeout)
    |> actor.start,
  )

  Ok(started.data)
}

/// The type used to indicate what to do with a resource after use.
pub opaque type Next(return) {
  /// Return the resource to the pool.
  Keep(return)
  /// Discard the resource, running the shutdown function on it.
  Discard(return)
}

/// Instruct Bath to keep the checked out resource, returning it to the pool.
pub fn keep() -> Next(Nil) {
  Keep(Nil)
}

/// Instruct Bath to discard the checked out resource, running the shutdown function on
/// it.
///
/// Discarded resources will be recreated lazily, regardless of the pool's creation
/// strategy.
pub fn discard() -> Next(Nil) {
  Discard(Nil)
}

/// Return a value from a use of [`apply`](#apply).
pub fn returning(next: Next(old), value: new) -> Next(new) {
  case next {
    Keep(_) -> Keep(value)
    Discard(_) -> Discard(value)
  }
}

/// Checks out a resource from the pool, sending the caller Pid for the pool to
/// monitor in case the client dies. This allows the pool to create a new resource
/// later if required.
fn check_out(
  pool: process.Subject(Msg(resource_type)),
  caller: Pid,
  timeout: Int,
) -> Result(resource_type, ApplyError) {
  process.call(pool, timeout, CheckOut(_, caller:))
}

fn check_in(
  pool: process.Subject(Msg(resource_type)),
  resource: resource_type,
  caller: Pid,
  next: Next(Nil),
) {
  process.send(pool, CheckIn(resource:, caller:, next:))
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
///
/// // Do stuff with resource...
///
/// // Return the resource to the pool, returning "Hello!" to the caller.
/// bath.keep()
/// |> bath.returning("Hello!")
/// ```
pub fn apply(
  pool pool: process.Subject(Msg(resource_type)),
  timeout timeout: Int,
  next next: fn(resource_type) -> Next(result_type),
) -> Result(result_type, ApplyError) {
  let self = process.self()
  use resource <- result.try(check_out(pool, self, timeout))

  let next_action = next(resource)

  let #(usage_result, next_action) = case next_action {
    Keep(return) -> #(return, Keep(Nil))
    Discard(return) -> #(return, Discard(Nil))
  }

  check_in(pool, resource, self, next_action)

  Ok(usage_result)
}

/// Like [`apply`](#apply), but blocks waiting for a resource if the pool is
/// exhausted instead of returning `NoResourcesAvailable` immediately.
///
/// The timeout covers both waiting in the queue and any internal pool operations.
///
/// ## Panics
///
/// This function will panic if the timeout expires before a resource becomes
/// available.
///
/// If you need to handle timeouts gracefully, you can:
/// - Use this function within a supervised process (let it crash, supervisor restarts)
/// - Wrap the call in a separate process/task that you can monitor
///
/// If the pool shuts down while waiting, `Error(PoolShuttingDown)` is returned.
///
/// ## Example
///
/// ```gleam
/// let assert Ok(pool) =
///   bath.new(fn() { Ok("Some pooled resource") })
///   |> bath.size(1)
///   |> bath.start(1000)
///
/// // This will block until a resource is available or panic on timeout
/// use resource <- bath.apply_blocking(pool, 5000)
///
/// // Do stuff with resource...
///
/// bath.keep()
/// |> bath.returning("Hello!")
/// ```
pub fn apply_blocking(
  pool pool: process.Subject(Msg(resource_type)),
  timeout timeout: Int,
  next next: fn(resource_type) -> Next(result_type),
) -> Result(result_type, ApplyError) {
  let self = process.self()
  use resource <- result.try(
    process.call(pool, timeout, CheckOutBlocking(_, caller: self)),
  )

  let next_action = next(resource)

  let #(usage_result, next_action) = case next_action {
    Keep(return) -> #(return, Keep(Nil))
    Discard(return) -> #(return, Discard(Nil))
  }

  check_in(pool, resource, self, next_action)

  Ok(usage_result)
}

/// Shut down the pool, calling the shutdown function on each
/// resource in the pool. Calling with `force` set to `True` will
/// force the shutdown, not calling the shutdown function on any
/// resources.
///
/// Will fail if there are still resources checked out, unless `force` is
/// `True`.
///
/// You only need to call this when using unsupervised pools. You should let your
/// supervision tree handle the shutdown of supervised resource pools.
pub fn shutdown(
  pool pool: process.Subject(Msg(resource_type)),
  force force: Bool,
  timeout timeout: Int,
) -> Result(Nil, ShutdownError) {
  process.call(pool, timeout, Shutdown(_, force))
}

// ----- Pool ----- //

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
    waiting: deque.Deque(WaitingRequest(resource_type)),
    selector: process.Selector(Msg(resource_type)),
    log_errors: Bool,
  )
}

type LiveResources(resource_type) =
  Dict(Pid, LiveResource(resource_type))

type LiveResource(resource_type) {
  LiveResource(resource: resource_type, monitor: process.Monitor)
}

type WaitingRequest(resource_type) {
  WaitingRequest(
    reply_to: Subject(Result(resource_type, ApplyError)),
    caller: Pid,
    monitor: process.Monitor,
  )
}

/// A message sent to the pool actor.
pub opaque type Msg(resource_type) {
  CheckIn(resource: resource_type, caller: Pid, next: Next(Nil))
  CheckOut(reply_to: Subject(Result(resource_type, ApplyError)), caller: Pid)
  CheckOutBlocking(
    reply_to: Subject(Result(resource_type, ApplyError)),
    caller: Pid,
  )
  PoolExit(process.ExitMessage)
  CallerDown(process.Down)
  WaiterDown(process.Down)
  Shutdown(reply_to: process.Subject(Result(Nil, ShutdownError)), force: Bool)
}

fn handle_pool_message(state: State(resource_type), msg: Msg(resource_type)) {
  case msg {
    CheckIn(resource:, caller:, next:) -> {
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

      case next {
        Keep(_) -> {
          case
            serve_next_waiter(
              state,
              Ok(resource),
              live_resources,
              selector,
              state.current_size,
            )
          {
            Served(live_resources:, waiting:, selector:, ..) ->
              State(..state, live_resources:, waiting:, selector:)
              |> actor.continue
              |> actor.with_selector(selector)
            NoneWaiting ->
              State(
                ..state,
                resources: deque.push_back(state.resources, resource),
                live_resources:,
                selector:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            ServeFailed(..) ->
              // Cannot happen: we passed Ok(resource), so creation is never attempted
              panic as "unreachable: serve_next_waiter with Ok resource cannot fail"
          }
        }
        Discard(_) -> {
          state.shutdown_resource(resource)
          let current_size = state.current_size - 1
          case
            serve_next_waiter(
              state,
              Error(Nil),
              live_resources,
              selector,
              current_size,
            )
          {
            Served(live_resources:, waiting:, selector:, current_size:) ->
              State(
                ..state,
                current_size:,
                live_resources:,
                waiting:,
                selector:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            ServeFailed(waiting:, selector:, current_size:) ->
              State(
                ..state,
                current_size:,
                live_resources:,
                waiting:,
                selector:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            NoneWaiting ->
              State(..state, current_size:, live_resources:, selector:)
              |> actor.continue
              |> actor.with_selector(selector)
          }
        }
      }
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
          State(
            ..state,
            resources: new_resources,
            current_size: new_current_size,
            selector:,
            live_resources:,
          )
          |> actor.continue
          |> actor.with_selector(selector)
        }
      }
    }
    CheckOutBlocking(reply_to:, caller:) -> {
      let get_result = case state.checkout_strategy {
        FIFO -> deque.pop_front(state.resources)
        LIFO -> deque.pop_back(state.resources)
      }

      let resource_result = case get_result {
        Ok(#(resource, new_resources)) ->
          Ok(#(resource, new_resources, state.current_size))
        Error(_) -> {
          case state.current_size < state.max_size {
            True -> {
              use resource <- result.try(
                state.create_resource()
                |> result.map_error(fn(err) {
                  log_resource_creation_error(state.log_errors, err)
                  CheckOutResourceCreateError(err)
                }),
              )
              Ok(#(resource, state.resources, state.current_size + 1))
            }
            False -> Error(NoResourcesAvailable)
          }
        }
      }

      case resource_result {
        Error(NoResourcesAvailable) -> {
          let monitor = process.monitor(caller)
          let selector =
            state.selector
            |> process.select_specific_monitor(monitor, WaiterDown)
          let waiting_request = WaitingRequest(reply_to:, caller:, monitor:)
          State(
            ..state,
            waiting: deque.push_back(state.waiting, waiting_request),
            selector:,
          )
          |> actor.continue
          |> actor.with_selector(selector)
        }
        Error(err) -> {
          actor.send(reply_to, Error(err))
          actor.continue(state)
        }
        Ok(#(resource, new_resources, new_current_size)) -> {
          let #(monitor, selector) = monitor_process(state.selector, caller)

          let live_resources =
            dict.insert(
              state.live_resources,
              caller,
              LiveResource(resource:, monitor:),
            )

          actor.send(reply_to, Ok(resource))
          State(
            ..state,
            resources: new_resources,
            current_size: new_current_size,
            selector:,
            live_resources:,
          )
          |> actor.continue
          |> actor.with_selector(selector)
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
          reject_waiters(state.waiting)

          state.resources
          |> deque.to_list
          |> list.each(state.shutdown_resource)

          actor.send(reply_to, Ok(Nil))
          actor.stop()
        }
        _, True -> {
          // Force shutdown
          reject_waiters(state.waiting)

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
          let live_resources =
            dict.delete(state.live_resources, process_down_pid)

          // Shutdown the old resource
          state.shutdown_resource(live_resource.resource)

          // current_size is unchanged here: we shutdown one resource above but
          // haven't decremented yet — serve_next_waiter will create a new one
          // if a waiter exists (net zero change), or we handle the no-waiter
          // case below with Eager/Lazy logic.
          let current_size = state.current_size
          case
            serve_next_waiter(
              state,
              Error(Nil),
              live_resources,
              selector,
              // Pass current_size - 1 because we destroyed the old resource.
              // If creation succeeds, serve_next_waiter adds 1 back (net zero).
              current_size - 1,
            )
          {
            Served(live_resources:, waiting:, selector:, current_size:) ->
              State(
                ..state,
                current_size:,
                live_resources:,
                waiting:,
                selector:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            ServeFailed(waiting:, selector:, current_size:) ->
              State(
                ..state,
                current_size:,
                live_resources:,
                waiting:,
                selector:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            NoneWaiting -> {
              let #(new_resources, new_current_size) = case
                state.creation_strategy
              {
                // If we create lazily, just decrement the current size - a new resource
                // will be created when required
                Lazy -> #(state.resources, current_size - 1)
                // Otherwise, create a new resource, warning if resource creation fails
                Eager -> {
                  case state.create_resource() {
                    // Destroyed one, created one — net zero change to size
                    Ok(resource) -> #(
                      deque.push_back(state.resources, resource),
                      current_size,
                    )
                    Error(resource_create_error) -> {
                      log_resource_creation_error(
                        state.log_errors,
                        resource_create_error,
                      )
                      #(state.resources, current_size - 1)
                    }
                  }
                }
              }

              State(
                ..state,
                resources: new_resources,
                current_size: new_current_size,
                selector:,
                live_resources:,
              )
              |> actor.continue
              |> actor.with_selector(selector)
            }
          }
        }
      }
    }
    WaiterDown(process_down) -> {
      let assert process.ProcessDown(pid: process_down_pid, ..) = process_down

      let #(new_waiting, monitors_to_remove) =
        state.waiting
        |> deque.to_list
        |> list.partition(fn(waiter) { waiter.caller != process_down_pid })
        |> fn(partitioned) {
          let #(kept, removed) = partitioned
          #(deque.from_list(kept), list.map(removed, fn(w) { w.monitor }))
        }

      let selector =
        list.fold(monitors_to_remove, state.selector, fn(sel, mon) {
          demonitor_process(sel, mon)
        })

      State(..state, waiting: new_waiting, selector:)
      |> actor.continue
      |> actor.with_selector(selector)
    }
  }
}

fn reject_waiters(waiting: deque.Deque(WaitingRequest(resource_type))) -> Nil {
  waiting
  |> deque.to_list
  |> list.each(fn(waiter) {
    process.demonitor_process(waiter.monitor)
    actor.send(waiter.reply_to, Error(PoolShuttingDown))
  })
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
  mapper: fn(Subject(Msg(resource_type))) -> return,
  init_timeout: Int,
) -> actor.Builder(State(resource_type), Msg(resource_type), return) {
  let pool_builder =
    actor.new_with_initialiser(init_timeout, fn(self) {
      use #(resources, current_size) <- result.try(create_pool_resources(
        builder,
      ))

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
          waiting: deque.new(),
          selector:,
          current_size:,
          max_size: builder.size,
          create_resource: builder.create_resource,
          shutdown_resource: builder.shutdown_resource,
          log_errors: builder.log_errors,
        )

      actor.initialised(state)
      |> actor.selecting(selector)
      |> actor.returning(mapper(self))
      |> Ok
    })
    |> actor.on_message(handle_pool_message)

  case builder.name {
    option.Some(name) -> actor.named(pool_builder, name)
    option.None -> pool_builder
  }
}

// ----- Utils ----- //

/// Result of attempting to serve a waiting request from the queue.
type ServeResult(resource_type) {
  /// A waiter was served successfully. Contains the updated state fields.
  Served(
    live_resources: LiveResources(resource_type),
    waiting: deque.Deque(WaitingRequest(resource_type)),
    selector: process.Selector(Msg(resource_type)),
    current_size: Int,
  )
  /// A waiter was found but resource creation failed.
  ServeFailed(
    waiting: deque.Deque(WaitingRequest(resource_type)),
    selector: process.Selector(Msg(resource_type)),
    current_size: Int,
  )
  /// No waiters in the queue.
  NoneWaiting
}

/// Try to pop the next waiter from the queue and serve them a resource.
///
/// If `resource` is `Ok`, that resource is handed to the waiter directly.
/// If `resource` is `Error`, a new resource is created for the waiter.
///
/// `current_size` should reflect the pool size *before* this resource is
/// accounted for (i.e. after the old resource was removed/shutdown).
fn serve_next_waiter(
  state: State(resource_type),
  resource: Result(resource_type, Nil),
  live_resources: LiveResources(resource_type),
  selector: process.Selector(Msg(resource_type)),
  current_size: Int,
) -> ServeResult(resource_type) {
  case deque.pop_front(state.waiting) {
    Error(_) -> NoneWaiting
    Ok(#(waiter, new_waiting)) -> {
      let resource_result = case resource {
        Ok(r) -> Ok(#(r, current_size))
        Error(_) ->
          case state.create_resource() {
            Ok(r) -> Ok(#(r, current_size + 1))
            Error(err) -> Error(err)
          }
      }
      case resource_result {
        Ok(#(r, new_current_size)) -> {
          let selector = demonitor_process(selector, waiter.monitor)
          let #(monitor, selector) = monitor_process(selector, waiter.caller)
          let live_resources =
            dict.insert(
              live_resources,
              waiter.caller,
              LiveResource(resource: r, monitor:),
            )
          actor.send(waiter.reply_to, Ok(r))
          Served(
            live_resources:,
            waiting: new_waiting,
            selector:,
            current_size: new_current_size,
          )
        }
        Error(err) -> {
          log_resource_creation_error(state.log_errors, err)
          let selector = demonitor_process(selector, waiter.monitor)
          actor.send(waiter.reply_to, Error(CheckOutResourceCreateError(err)))
          ServeFailed(waiting: new_waiting, selector:, current_size:)
        }
      }
    }
  }
}

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
  process.demonitor_process(monitor)
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
