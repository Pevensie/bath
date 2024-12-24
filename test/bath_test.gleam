import bath
import gleam/io
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

// gleeunit test functions end in `_test`
pub fn lifecycle_test() {
  let assert Ok(pool) = bath.init(10, fn() { Ok(10) })
  let assert Ok(_) = bath.apply(pool, 1000, fn(n) { io.debug(n) })
  let assert Ok(_) = bath.shutdown(pool, fn(_) { Nil }, 1000)
}

pub fn empty_pool_fails_to_apply_test() {
  let assert Ok(pool) = bath.init(0, fn() { Ok(10) })
  let assert Error(bath.NoResourcesAvailable) =
    bath.apply(pool, 1000, fn(_) { Nil })
  let assert Ok(_) = bath.shutdown(pool, fn(_) { Nil }, 1000)
}

pub fn pool_has_correct_capacity_test() {
  let assert Ok(pool) = bath.init(1, fn() { Ok(10) })
  let assert Ok(_) =
    bath.apply(pool, 1000, fn(_) {
      // Only one capacity, so attempting to check out another resource
      // should fail
      let assert Error(bath.NoResourcesAvailable) =
        bath.apply(pool, 1000, fn(_) { Nil })
      Nil
    })
  let assert Ok(_) = bath.shutdown(pool, fn(_) { Nil }, 1000)
}

pub fn pool_has_correct_resources_test() {
  let assert Ok(pool) = bath.init(1, fn() { Ok(10) })
  let assert Ok(_) =
    bath.apply(pool, 1000, fn(n) {
      // Check we have the right values
      n
      |> should.equal(10)
    })
  let assert Ok(_) = bath.shutdown(pool, fn(_) { Nil }, 1000)
}
