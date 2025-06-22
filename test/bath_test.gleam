import bath
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/io
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleeunit
import gleeunit/should
import logging

pub fn main() {
  logging.configure()
  gleeunit.main()
}

// gleeunit test functions end in `_test`
pub fn lifecycle_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.on_shutdown(fn(res) {
      io.println("Shutting down with resource: " <> int.to_string(res))
      Nil
    })
    |> bath.start(1000)
  let assert Ok(20) =
    bath.apply(pool, 1000, fn(n) { bath.keep() |> bath.returning(n * 2) })
  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn supervised_lifecycle_test() {
  let pool_name = process.new_name("bath_pool")

  let bath_child_spec =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.name(pool_name)
    |> bath.supervised(1000)

  let assert Ok(_started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(bath_child_spec)
    |> static_supervisor.start

  let pool = process.named_subject(pool_name)

  let assert Ok(_) = bath.shutdown(pool, False, 1000)
}

pub fn empty_pool_fails_to_apply_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  process.spawn(fn() {
    use _ <- bath.apply(pool, 1000)
    process.sleep(1000)
    bath.keep()
  })

  process.sleep(100)

  let assert Error(bath.NoResourcesAvailable) =
    bath.apply(pool, 1000, fn(_) { bath.keep() })

  let assert Ok(Nil) = bath.shutdown(pool, True, 1000)
}

pub fn pool_has_correct_capacity_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)
  let assert Ok(_) =
    bath.apply(pool, 1000, fn(_) {
      // Only one capacity, so attempting to check out another resource
      // should fail
      let assert Error(bath.NoResourcesAvailable) =
        bath.apply(pool, 1000, fn(_) { bath.keep() })
      bath.keep()
    })
  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn pool_has_correct_resources_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(10)
    |> bath.start(1000)

  let assert Ok(_) =
    bath.apply(pool, 1000, fn(n) {
      // Check we have the right values
      n
      |> should.equal(10)

      bath.keep()
    })

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn pool_handles_caller_crash_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  // Expect an error message here
  logging.set_level(logging.Critical)

  process.spawn_unlinked(fn() {
    use _ <- bath.apply(pool, 1000)
    panic as "Oh no, the caller crashed!"
  })

  process.sleep(100)

  // Reset level
  logging.configure()

  // Ensure the pool still has an available resource
  let assert Ok(10) =
    bath.apply(pool, 1000, fn(r) { bath.keep() |> bath.returning(r) })

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn eager_pool_fails_to_start_if_resource_creation_fails_test() {
  let assert Error(actor.InitFailed("Failed to create resource")) =
    bath.new(fn() { Error("Failed to create resource") })
    |> bath.creation_strategy(bath.Eager)
    |> bath.start(1000)
}

pub fn shutdown_function_gets_called_test() {
  let self = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.creation_strategy(bath.Eager)
    |> bath.on_shutdown(fn(_) { process.send(self, "Shut down") })
    |> bath.start(1000)

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)

  let assert Ok("Shut down") = process.receive(self, 1000)
}

pub fn shutdown_function_gets_called_on_discard_test() {
  let self = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.creation_strategy(bath.Eager)
    |> bath.on_shutdown(fn(_) { process.send(self, "Shut down") })
    |> bath.start(1000)

  let assert Ok(_) = bath.apply(pool, 1000, fn(_) { bath.discard() })

  let assert Ok("Shut down") = process.receive(self, 1000)

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn shutdown_function_doesnt_get_called_on_keep_test() {
  let self = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.creation_strategy(bath.Eager)
    |> bath.on_shutdown(fn(_) { process.send(self, "Shut down") })
    |> bath.start(1000)

  let assert Ok(_) = bath.apply(pool, 1000, fn(_) { bath.keep() })

  let assert Error(Nil) = process.receive(self, 100)

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

// ----- Util tests  ----- //

pub fn try_map_returning_succeeds_if_no_errors_test() {
  let list = [Ok(1), Ok(2), Ok(3)]

  let assert Ok([1, 2, 3]) = bath.try_map_returning(list, function.identity)
}

pub fn try_map_returning_fails_early_if_any_errors_test() {
  let list = [Ok(1), Error("error"), Ok(3)]

  let assert Error(#([1], "error")) =
    bath.try_map_returning(list, function.identity)
}
