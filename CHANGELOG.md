## [2.1.0] 2017-10-03
### Changed
* Changed the default GC settings, less heap, more reserved percentage, works better with large requests.
* Upgrade Hive from 2.1.0 to 2.3.0.
* Depend on latest parent with test.arguments build parameter
* Fixed bug where tunnel configuration wasn't being applied.
* Hive dependency updated to 2.1.1 (Needed corresponding BeeJU dependency update as well)
### Removed
* Removed SessionFactorySupplierFactory.

## [2.0.3] 2017-09-25 [YANKED]

## [2.0.2] 2017-08-01
### Added
* Flag to prevent flushing the federation configuration out when the server stops.
### Changed
* Configure Maven deploy plug-in.

## [2.0.1] 2017-07-27
### Changed
* Few extra notes in README.

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
