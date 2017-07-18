## [2.0.0]
### Added
* Allow database name patterns in DDL.
### Changed
* NOTE: Backward incompatible config change. Renamed graphite config `inter-poll-time` and `inter-poll-time-unit` to `poll-interval` and `poll-interval-time-unit`.
* Changed default rpm user from 'hadoop' to 'waggle-dance'. If not present 'root' will be used.

## [1.1.2]
### Added
* Expose metastore status in the REST endpoint.

## [1.1.1]
### Added
* Support for `hive.metastore.execute.setugi=true` hive flag, UGI is now correctly sent along to federated metastores.

## [1.1.0]
### Changed
* Prefix is now optional, the metastore name can be used to derive it by default if needed.
* Improved metastore client connection logic.

## [1.0.1]
### Added
* Adding test and improved integration tests running time.
### Changed
* Improved client-side error message.

## [1.0.0]
### Added
* Functionality to avoid prefixes and allow for a fixed list of federated databases to be configured.
* Externalized log4j2.xml so we can change log levels.
* Support for more flexible primary metastore configuration including database whitelist access controls.
* Metastore metrics and Graphite support.
### Fixed
* Bug where client exceptions were not properly propagated.
* Small bugfix where we threw the wrong thrift Exception (TException).

## [0.0.4]
### Fixed
* Fix active connections counter.

## [0.0.3]
### Added
* Support for SHH tunneling on federated metastores.
### Fixed
* Fix memory leak.
