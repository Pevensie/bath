# 🛁 Bath

[![Package Version](https://img.shields.io/hexpm/v/bath)](https://hex.pm/packages/bath)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/bath/)

Bath is a generic resource pool for Gleam. It can be used to manage a pool of
any value, such as database connections, file handles, or other resources.

## Installation

```sh
gleam add bath
```

## Usage

The recommended way to start a Bath pool is with the `supervised` function. You
can use this to include the Bath pool in your application's supervision tree.

```gleam
import bath
import fake_db
import gleam/otp/static_supervisor as supervisor

pub fn main() {
  // Create a subject to receive the pool handler once the supervision tree has been
  // started. Use a named subject to make sure we can always receive the pool handler,
  // even if our original process crashes.
  let pool_receiver_name = process.new_name("bath_pool_receiver")
  let assert Ok(_) = process.register(process.self(), pool_receiver_name)

  let pool_receiver = process.named_subject(pool_receiver_name)

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

    // Return the connection to the pool, returning "Hello!" to the caller.
    bath.keep()
    |> bath.returning("Hello!")
  }

  // Do more stuff...
}
```

Further documentation can be found at <https://hexdocs.pm/bath>.

## Development

If you've found any bugs, please open an issue on
[GitHub](https://github.com/Pevensie/bath/issues).

The code is reasonably well tested and documented, but PRs to improve either are always
welcome.

```sh
gleam test  # Run the tests
```
