# Changelog

## v6.0.0 - 2026-02-20

- Added the `apply_blocking` function (thanks to [jtdowney](https://github.com/jtdowney)!).
  - New error variant, hence the breaking change.

## v5.0.0 - 2025-06-22

- Remove the `Pool` type in favour of `process.Subject(bath.Msg(msg))`. It was just a
  wrapper, anyway.
- Allow pools to be named, avoiding the previous dance required to start a supervised
  pool.
  - This changes the signature of `bath.supervised` and `bath.supervised_map`.

## v4.1.0 - 2025-06-21

- Added `bath.supervised_map` to create a supervised pool of resources while mapping
  the `Pool` value to a new type.
- Fixed an issue where Bath would fail to demonitor processes once a resource had been
  checked back into the pool.

## v4.0.0 - 2025-06-20

- The function passed to `bath.apply` must now return a `bath.Next(return)` value to
  indicate whether the checked-out resource should be returned to the pool or
  discarded. This allows users to control the lifecycle of resources more precisely.
  For example, you can now shut down and dispose of pooled TCP sockets for which the
  connection has been closed.

## v3.0.0 - 2025-06-14

Bath has been updated to use the new stable versions of `gleam/erlang` and
`gleam/otp`. Bath now follows the conventions laid out in `gleam/otp`, and
creating a resource pool under a supervisor is now much easier.

### Example

The recommended way to start a Bath pool is with the `supervised` function. You
can use this to include the Bath pool in your application's supervision tree.

```gleam
import bath
import fake_db
import gleam/otp/static_supervisor as supervisor

pub fn main() {
  let pool_receiver = process.new_subject()

  // Define a pool of 10 connections to some fictional database, and create a child
  // spec to allow it to be supervised.
  let bath_child_spec =
    bath.new(fn() { fake_db.get_conn() })
    |> bath.size(10)
    |> bath.supervised(pool_receiver, 1000)

  // Start the pool under a supervisor
  let assert Ok(_started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(bath_child_spec)
    |> supervisor.start

  // Receive the pool handle now that it's started
  let assert Ok(pool) = process.receive(pool_receiver, 1000)

  // Use the pool. Shown here in a block to use `use`.
  let assert Ok("Hello!") = {
    use conn <- bath.apply(pool, 1000)
    // Do stuff with the connection...
    "Hello!"
  }

  // Close the pool.
  let assert Ok(_) = bath.shutdown(pool, False, 1000)
}
```

### Behavioural changes

#### Panics

Like the new version of `gleam/erlang`, failing to send messages to the Bath pool will
will now panic rather than returning an error result.

##### Why?

Previously, Bath used the `process.try_call` function that was present in
`gleam/erlang`. However, this had the potential to cause a memory leak if the
process did not return within the provided timeout.

The calling process would cancel its receive operation and continue, and the
process would also continue its operation. When the process replied to the
calling process, that message would be stuck in the caller's queue, never to
be received.

## v2.0.0 - 2024-12-29

- Switch to a builder pattern for pool configuration.
- Change the `shutdown` API to take a `force` argument instead of having a separate
  `force_shutdown` function.
- Improve reliability by tracking live resources and monitoring the calling process,
  re-adding resources to the pool if the caller dies.
- Add checkout strategies (`FIFO` and `LIFO`).
- Add resource creation strategies (`Lazy` and `Eager`).
- Added `child_spec` and `from_subject` functions for creating a pool as part of a
  supervision tree.

Thanks to @lpil for their help debugging this release!

## v1.0.0 - 2024-12-12

- Initial release
