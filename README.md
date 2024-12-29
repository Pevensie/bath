# 🛁 Bath

[![Package Version](https://img.shields.io/hexpm/v/bath)](https://hex.pm/packages/bath)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/bath/)

Bath is a generic resource pool for Gleam. It can be used to manage a pool of
any value, such as database connections, file handles, or other resources.

## Installation

```sh
gleam add bath@2
```

## Usage

```gleam
import bath
import fake_db

pub fn main() {
  // Create a pool of 10 connections to some fictional database.
  let assert Ok(pool) =
    bath.new(fn() { fake_db.connect() })
    |> bath.with_size(10)
    |> bath.with_shutdown(fn(conn) { fake_db.close(conn) })
    |> bath.start(1000)

  // Use the pool. Shown here in a block to use `use`.
  let assert Ok("Hello!") = {
    use conn <- bath.apply(pool, 1000)
    // Do stuff with the connection...
    "Hello!"
  }

  // Close the pool.
  let assert Ok(_) = bath.shutdown(pool, fn(conn) { fake_db.close(conn) }, 1000)
}
```

Further documentation can be found at <https://hexdocs.pm/bath>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```
