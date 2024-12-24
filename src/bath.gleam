import gleam/dynamic
import gleam/erlang/process
import gleam/list
import gleam/otp/actor
import gleam/result

type Msg(resource_type) {
  CheckIn(resource_type)
  CheckOut(process.Subject(Result(resource_type, ApplyError)))
  Shutdown(
    fn(resource_type) -> Nil,
    process.Subject(Result(Nil, ShutdownError)),
  )
  ForceShutdown(fn(resource_type) -> Nil)
}

type PoolSubject(resource_type) =
  process.Subject(Msg(resource_type))

type PoolState(resource_type) {
  PoolState(checked_in: List(resource_type), checked_out: List(resource_type))
}

/// A resource pool.
pub opaque type Pool(resource_type) {
  Pool(size: Int, subject: PoolSubject(resource_type))
}

/// An error returned when creating a [`Pool`](#Pool).
pub type InitError(resource_create_error) {
  /// The actor failed to start.
  StartError(actor.StartError)
  /// The resource creation function failed.
  ResourceCreateError(resource_create_error)
}

/// An error returned when the resource pool fails to shut down.
pub type ShutdownError {
  /// There are still resources checked out. Ignore this failure case by
  /// calling [`force_shutdown`](#force_shutdown) function.
  ResourcesInUse
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
) -> Result(Pool(resource_type), InitError(resource_create_error)) {
  let resources_result =
    list.repeat("", size)
    |> list.try_map(fn(_) {
      resource_create_function() |> result.map_error(ResourceCreateError)
    })

  use resources <- result.try(resources_result)

  let actor_result =
    actor.start(
      PoolState(checked_in: resources, checked_out: []),
      handle_message,
    )
    |> result.map_error(StartError)

  use subject <- result.try(actor_result)
  Ok(Pool(size:, subject:))
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
) {
  use resource <- result.try(check_out(pool.subject, timeout))
  let result = next(resource)
  check_in(pool.subject, resource)
  Ok(result)
}

/// Shut down the pool, calling the `resource_shutdown_function` on each
/// resource in the pool.
///
/// Will fail if there are still resources checked out.
pub fn shutdown(
  pool: Pool(resource_type),
  resource_shutdown_function: fn(resource_type) -> Nil,
  timeout: Int,
) {
  process.try_call(
    pool.subject,
    Shutdown(resource_shutdown_function, _),
    timeout,
  )
  |> result.map_error(fn(err) {
    case err {
      process.CallTimeout -> ShutdownTimeout
      process.CalleeDown(reason) -> CalleeDown(reason:)
    }
  })
  |> result.flatten
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
  process.send(pool.subject, ForceShutdown(resource_shutdown_function))
}

fn check_out(pool_subject: PoolSubject(resource_type), timeout: Int) {
  process.try_call(pool_subject, CheckOut, timeout)
  |> result.replace_error(CheckoutTimeout)
  |> result.flatten
}

fn check_in(
  pool_subject: PoolSubject(resource_type),
  item: resource_type,
) -> Nil {
  process.send(pool_subject, CheckIn(item))
}

fn handle_message(msg: Msg(resource_type), pool_state: PoolState(resource_type)) {
  case msg {
    CheckIn(resource) -> {
      let checked_out =
        pool_state.checked_out
        |> list.filter(fn(item) { item != resource })

      let checked_in = [resource, ..pool_state.checked_in]
      actor.continue(PoolState(checked_in:, checked_out:))
    }
    CheckOut(client) -> {
      case pool_state.checked_in {
        [] -> {
          actor.send(client, Error(NoResourcesAvailable))
          actor.continue(pool_state)
        }
        [chosen, ..checked_in] -> {
          actor.send(client, Ok(chosen))
          actor.continue(
            PoolState(checked_in:, checked_out: [
              chosen,
              ..pool_state.checked_out
            ]),
          )
        }
      }
    }
    Shutdown(resource_shutdown_function, reply_to) -> {
      case pool_state.checked_out {
        [] -> {
          pool_state.checked_in
          |> list.each(resource_shutdown_function)

          process.send(reply_to, Ok(Nil))
          actor.Stop(process.Normal)
        }
        _ -> {
          process.send(reply_to, Error(ResourcesInUse))
          actor.continue(pool_state)
        }
      }
    }
    ForceShutdown(resource_shutdown_function) -> {
      list.append(pool_state.checked_in, pool_state.checked_out)
      |> list.each(resource_shutdown_function)

      actor.Stop(process.Normal)
    }
  }
}
