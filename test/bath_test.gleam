import bath
import gleam/erlang/process
import gleam/io
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
    |> bath.with_size(1)
    |> bath.with_shutdown(fn(res) {
      io.debug("Shutting down")
      io.debug(res)
      Nil
    })
    |> bath.start(1000)
  let assert Ok(20) = bath.apply(pool, 1000, fn(n) { n * 2 })
  bath.shutdown(pool)
}

pub fn empty_pool_fails_to_apply_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.with_size(0)
    |> bath.start(1000)
  let assert Error(bath.NoResourcesAvailable) =
    bath.apply(pool, 1000, fn(_) { Nil })
  bath.shutdown(pool)
}

pub fn pool_has_correct_capacity_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.with_size(1)
    |> bath.start(1000)
  let assert Ok(_) =
    bath.apply(pool, 1000, fn(_) {
      // Only one capacity, so attempting to check out another resource
      // should fail
      let assert Error(bath.NoResourcesAvailable) =
        bath.apply(pool, 1000, fn(_) { Nil })
      Nil
    })
  bath.shutdown(pool)
}

pub fn pool_has_correct_resources_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.with_size(10)
    |> bath.start(1000)

  let assert Ok(_) =
    bath.apply(pool, 1000, fn(n) {
      // Check we have the right values
      n
      |> should.equal(10)
    })

  bath.shutdown(pool)
}

pub fn pool_handles_caller_crash_test() {
  let assert Ok(pool) =
    bath.new(fn() { Ok(10) })
    |> bath.with_size(1)
    |> bath.start(1000)

  // Expect an error message here
  logging.set_level(logging.Critical)

  process.start(
    fn() {
      use _ <- bath.apply(pool, 1000)
      panic as "Oh no, the caller crashed!"
    },
    False,
  )

  process.sleep(100)

  // Reset level
  logging.configure()

  // Ensure the pool still has an available resource
  let assert Ok(10) = bath.apply(pool, 1000, fn(r) { r })

  bath.shutdown(pool)
}
