# Changelog

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
