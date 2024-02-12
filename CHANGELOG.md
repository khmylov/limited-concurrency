# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2023-09-13
### Changed
- `LimitedParallelExecutor.ExecuteAsync` and `ConcurrentPartitioner.ExecuteAsync` now create `Task` with `TaskCreationOptions.RunContinuationsAsynchronously`
  - Previous implementation causes deadlocks in some cases
  - Technically a **breaking change**
- Decrease allocations in `ConcurrentPartitioner`

## [2.1.0] - 2022-12-16
### Changed
- Decrease allocations by caching delegates
- Expose `LimitedParallelExecutor.CurrentlyRunningCount`

## [2.0.0] - 2022-08-26
### Changed
- Move result type parameter from `ConcurrentPartitioner` class to its `ExecuteAsync` method to simplify usage.

## [1.1.0] - 2021-08-04
### Added
- Allow configuring max concurrency per partition

## [1.0.2] - 2021-08-04
### Changed
- FIX [#1](https://github.com/khmylov/limited-concurrency/issues/1): `ConcurrentPartitioner.CurrentPartitionCount` may become negative
  - This should also fix an issue when more than 1 concurrent action may be executed per partition at a single point in time.
  - In scope of this change, internal locks around the dictionary were also removed, which results in faster execution of highly concurrent scenarios.

## [1.0.1] - 2021-07-29
### Added
- Include doc file and source symbols into the package
- Mark `ConcurrentPartitioner._currentPartitionCount` as `volatile`

