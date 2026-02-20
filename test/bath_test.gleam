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

pub fn apply_blocking_waits_for_resource_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  let result_subject = process.new_subject()

  // First caller takes the only resource
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(200)
    bath.keep()
  })

  process.sleep(50)

  // Second caller uses apply_blocking - should wait and succeed
  process.spawn(fn() {
    let result =
      bath.apply_blocking(pool, 5000, fn(r) { bath.keep() |> bath.returning(r) })
    process.send(result_subject, result)
  })

  let assert Ok(Ok(10)) = process.receive(result_subject, 1000)
  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn apply_blocking_timeout_panics_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  let result_subject = process.new_subject()

  // Take the only resource and hold it
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(1000)
    bath.keep()
  })

  process.sleep(50)

  // Try to get resource with short timeout in separate process
  // It should panic (process dies)
  let pid =
    process.spawn_unlinked(fn() {
      bath.apply_blocking(pool, 100, fn(_) { bath.keep() })
      |> Ok
      |> process.send(result_subject, _)
    })

  let monitor = process.monitor(pid)

  let assert Ok(process.ProcessDown(..)) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(500)

  let assert Ok(Nil) = bath.shutdown(pool, True, 1000)
}

pub fn apply_blocking_fifo_order_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  let order_subject = process.new_subject()

  // First caller takes the only resource
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(300)
    bath.keep()
  })

  process.sleep(50)

  // Second and third callers queue up
  process.spawn(fn() {
    use _ <- bath.apply_blocking(pool, 5000)
    process.send(order_subject, "second")
    bath.keep()
  })

  process.sleep(50)

  process.spawn(fn() {
    use _ <- bath.apply_blocking(pool, 5000)
    process.send(order_subject, "third")
    bath.keep()
  })

  let assert Ok("second") = process.receive(order_subject, 2000)
  let assert Ok("third") = process.receive(order_subject, 2000)
  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn apply_blocking_shutdown_rejects_waiters_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  let result_subject = process.new_subject()

  // Take the only resource
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(500)
    bath.keep()
  })

  process.sleep(50)

  // Queue up a waiter
  process.spawn(fn() {
    let result = bath.apply_blocking(pool, 5000, fn(_) { bath.keep() })
    process.send(result_subject, result)
  })

  process.sleep(50)

  // Force shutdown while waiter is waiting
  let assert Ok(Nil) = bath.shutdown(pool, True, 1000)

  // Waiter should receive PoolShuttingDown error
  let assert Ok(Error(bath.PoolShuttingDown)) =
    process.receive(result_subject, 1000)
}

pub fn apply_blocking_waiter_crash_removes_from_queue_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.size(1)
    |> bath.start(1000)

  let result_subject = process.new_subject()

  logging.set_level(logging.Critical)

  // Take the only resource
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(300)
    bath.keep()
  })

  process.sleep(50)

  // First waiter will crash
  process.spawn_unlinked(fn() {
    use _ <- bath.apply_blocking(pool, 5000)
    panic as "Waiter crashed!"
  })

  process.sleep(50)

  // Second waiter should still get the resource
  process.spawn(fn() {
    let result =
      bath.apply_blocking(pool, 5000, fn(r) { bath.keep() |> bath.returning(r) })
    process.send(result_subject, result)
  })

  logging.configure()

  // Second waiter should succeed
  let assert Ok(Ok(10)) = process.receive(result_subject, 2000)
  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn apply_blocking_with_discard_creates_new_resource_for_waiter_test() {
  let creation_counter = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() {
      process.send(creation_counter, Nil)
      Ok(10)
    })
    |> bath.size(1)
    |> bath.start(1000)

  let result_subject = process.new_subject()

  // First caller takes the only resource and discards it
  process.spawn(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(200)
    bath.discard()
  })

  process.sleep(50)

  // Second caller blocks waiting
  process.spawn(fn() {
    let result =
      bath.apply_blocking(pool, 5000, fn(r) { bath.keep() |> bath.returning(r) })
    process.send(result_subject, result)
  })

  // Waiter should get a freshly created resource after the discard
  let assert Ok(Ok(10)) = process.receive(result_subject, 1000)

  // Drain the creation counter: one for original checkout, one for the waiter
  let assert Ok(Nil) = process.receive(creation_counter, 100)
  let assert Ok(Nil) = process.receive(creation_counter, 100)

  let assert Ok(Nil) = bath.shutdown(pool, False, 1000)
}

pub fn lazy_defers_resource_creation_on_crash_test() {
  // Track how many times create_resource is called
  let counter = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() {
      process.send(counter, Nil)
      Ok(10)
    })
    |> bath.size(1)
    |> bath.creation_strategy(bath.Lazy)
    |> bath.start(1000)

  // Lazy: no resource created at start
  let assert Error(Nil) = process.receive(counter, 100)

  // Checkout creates resource on demand
  process.spawn_unlinked(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(100)
    panic as "Holder crashed"
  })

  // First creation happens on checkout
  let assert Ok(Nil) = process.receive(counter, 500)

  // Wait for crash to be processed
  process.sleep(200)

  // Lazy should NOT create replacement when no waiters
  let assert Error(Nil) = process.receive(counter, 50)
  let assert Ok(Nil) = bath.shutdown(pool, True, 1000)
}

pub fn eager_replaces_resource_on_crash_test() {
  // Track how many times create_resource is called
  let counter = process.new_subject()

  let assert Ok(pool) =
    bath.new(fn() {
      process.send(counter, Nil)
      Ok(10)
    })
    |> bath.size(1)
    |> bath.creation_strategy(bath.Eager)
    |> bath.start(1000)

  // Eager: resource created at start
  let assert Ok(Nil) = process.receive(counter, 500)

  // Checkout uses existing resource (no new creation)
  process.spawn_unlinked(fn() {
    use _ <- bath.apply(pool, 5000)
    process.sleep(200)
    panic as "Holder crashed"
  })

  // Wait for checkout to complete, then verify no new creation yet
  process.sleep(100)
  let assert Error(Nil) = process.receive(counter, 50)

  // Wait for crash to be processed
  process.sleep(100)

  // Eager SHOULD create replacement when no waiters
  let assert Ok(Nil) = process.receive(counter, 500)
  let assert Ok(Nil) = bath.shutdown(pool, True, 1000)
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
