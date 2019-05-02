## TBD
### Fixed
* Fixed REST API (http://localhost:18000/api/admin/federations/) which broke in 3.3.0 release.

## [3.3.0] - 2019-04-30
### Fixed
* Reconnection to metastores for MANUAL database resolution. With this change, the server configuration can specify a `status-polling-delay` and `status-polling-delay-time-unit`. See the [README](https://github.com/HotelsDotCom/waggle-dance#server) for more information.

### Changed
* Allow primary metastore to have a prefix. See [#152](https://github.com/HotelsDotCom/waggle-dance/issues/152).

## [3.2.0] - 2019-03-27
### Added
* Configurable `latency` for each metastore in a Waggle Dance configuration.

### Fixed
* Support for regex `mapped-databases` for MANUAL database resolution. See [#147](https://github.com/HotelsDotCom/waggle-dance/issues/147).
* Avoid NPE when no elements are provided for `mapped-databases` in the configuration. See [#131](https://github.com/HotelsDotCom/waggle-dance/issues/131).
* Support for running metastore queries in parallel.
* Support request without DbName like BitSetCheckedAuthorizationProvider#authorizeUserPrivcannot. See [#158](https://github.com/HotelsDotCom/waggle-dance/issues/158)

### Changed
* Updated `hotels-oss-parent` to 4.0.1 (was 2.3.5).
* Added invocation log messages for `getPartitions` calls.

## [3.1.2] - 2019-01-11
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Updated `hotels-oss-parent` to 2.3.5 (was 2.3.3).

### Fixed
* Fixed compatibility layer exception handling.

## [3.1.1] - 2018-10-30
### Fixed
* Issue where setting strict-host-key-checking for metastore-tunnel causes an error. See [#145](https://github.com/HotelsDotCom/waggle-dance/issues/145).

## [3.1.0] - 2018-10-25
### Changed
* Refactored general metastore tunnelling code to leverage hcommon-hive-metastore libraries. See [#103](https://github.com/HotelsDotCom/waggle-dance/issues/103).

### Fixed
* Fixed IllegalArgumentException thrown while doing write operations on federated metastores in MANUAL database resolution. See [141](https://github.com/HotelsDotCom/waggle-dance/issues/141).
* Compatibility issue with `get_foreign_keys()` and `get_primary_keys()` methods.

## [3.0.0] - 2018-09-20
### Changed
* Minimum supported Java version is now 8 (was 7). See [#108](https://github.com/HotelsDotCom/waggle-dance/issues/108).
* Waggle Dance updated to use Spring-Boot-2.0.4 instead of Spring Platform BOM. See [#105](https://github.com/HotelsDotCom/waggle-dance/issues/105).
* Changed JVM Metrics, the following modifications are needed if you are tracking these metrics and want to achieve equivalency:
    * `memory.heap.used` -> `sum(jvm.memory.used.area.heap.id.*)` - i.e. you now need to sum up all the heap spaces
    * `memory.heap.max` -> `sum(jvm.memory.max.area.heap.id.*)` - i.e. you now need to sum up all the max used heap spaces
    * `threads.*` -> `jvm.threads.*`

### Fixed
* Added workaround when federating to a Hive 1.x Metastore. See [#110](https://github.com/HotelsDotCom/waggle-dance/issues/110).

## [2.4.2] - 2018-08-21
### Changed
* Removed performance hit we get from checking if a connection is alive for non-tunneled connections. See [#115](https://github.com/HotelsDotCom/waggle-dance/issues/115).
* Removed System.exit calls from the service instead it will exit with an exception if the Spring Boot exit code is not 0.

## [2.4.1] - 2018-08-10
### Changed
* Updated `hotels-oss-parent` to version 2.3.3 (was 2.3.2).

## [2.4.0] - 2018-07-27
### Added
* Enable federated metastore's access-control-type to be configured to `READ_ONLY` and `READ_AND_WRITE_ON_DATABASE_WHITELIST`. See [#87](https://github.com/HotelsDotCom/waggle-dance/issues/87).

### Changed
* Refactored general purpose Hive metastore code to leverage [hcommon-hive-metastore](https://github.com/HotelsDotCom/hcommon-hive-metastore) and [hcommon-ssh](https://github.com/HotelsDotCom/hcommon-ssh) libraries. See [#78](https://github.com/HotelsDotCom/waggle-dance/issues/78).
* Updated versions of dependencies and plugins in waggle-dance-parent, waggle-dance, waggle-dance-core and waggle-dance-rpm modules. 
* Upgraded default Hive version from 2.3.0 to 2.3.3.

## [2.3.7] - 2018-06-19
### Fixed
* Silently handling parse errors in views. See [#83](https://github.com/HotelsDotCom/waggle-dance/issues/83).
* Double backticks in View queries. See [#84](https://github.com/HotelsDotCom/waggle-dance/issues/84).

## [2.3.6] - 2018-06-11
### Changed
* Client creation exceptions are caught (exceptions seem mostly due to tunneling) and no longer affect the whole of WD, unreachable Metastore is ignored. See [#80](https://github.com/HotelsDotCom/waggle-dance/issues/80).

## [2.3.5] - 2018-05-22
### Changed
* Using hcommon-ssh-1.0.1 dependency to fix issue where metastore exceptions were lost and not propagated properly over tunnelled connections.

### Fixed
* Issue where WD is unresponsive when a tunneled metastore connection becomes unreachable. See [#73](https://github.com/HotelsDotCom/waggle-dance/issues/73).

## [2.3.4] - 2018-05-16 
### Fixed
* View query parsing code shouldn't use JRE class. See [#62](https://github.com/HotelsDotCom/waggle-dance/issues/74).

## [2.3.3] - 2018-05-14
### Fixed
* Issue where not all views where correctly transformed. See [#62](https://github.com/HotelsDotCom/waggle-dance/issues/62).

## [2.3.2] - 2018-05-03
### Added
* Regex support in `federated-meta-stores.mapped-databases`. [#59](https://github.com/HotelsDotCom/waggle-dance/issues/59).

### Changed
* Replace SSH support with [hcommon-ssh](https://github.com/HotelsDotCom/hcommon-ssh) library. [#51](https://github.com/HotelsDotCom/waggle-dance/issues/51).

### Fixed
* Tables referenced in views are now correctly transformed to the context of the Waggle Dance client. See [#62](https://github.com/HotelsDotCom/waggle-dance/issues/62).

## [2.3.1] - 2018-04-09
### Added
* Configure StrictHostKeyChecking for MetastoreTunnel in YAML configuration. See [#33](https://github.com/HotelsDotCom/waggle-dance/issues/33).

### Fixed
* DESCRIBE FORMATTED query against federated tables now works. See [#60](https://github.com/HotelsDotCom/waggle-dance/issues/60).

## [2.3.0] - 2018-03-22
### Added
* Configurable SSH session timeout for SSH tunnels. See [#49](https://github.com/HotelsDotCom/waggle-dance/issues/49).
* Regexes enabled in writable database whitelist. See [#43](https://github.com/HotelsDotCom/waggle-dance/issues/43).
* Database whitelisting capabilities on `PREFIXED` mode. See [#47](https://github.com/HotelsDotCom/waggle-dance/issues/47).

## [2.2.2] - 2017-12-01
### Fixed
* Metastore status check now works for tunneled connections. See [#34](https://github.com/HotelsDotCom/waggle-dance/issues/34).

## [2.2.1] - 2017-10-30
### Fixed
* Metastore status was missing from api/admin/federations rest endpoint. See [#29](https://github.com/HotelsDotCom/waggle-dance/issues/29).

## [2.2.0] - 2017-10-05
### Changed
* `DatabaseMapping`s no longer make copies of the Thrift objects, it mutates the original objects instead.
* Upgrade Spring, BeeJU and other dependencies.

## [2.1.0] - 2017-10-03
### Changed
* Changed the default GC settings, less heap, more reserved percentage, works better with large requests.
* Upgrade Hive from 2.1.0 to 2.3.0.
* Depend on latest parent with `test.arguments` build parameter.
* Fixed bug where tunnel configuration wasn't being applied.
* Hive dependency updated to 2.1.1 (Needed corresponding BeeJU dependency update as well).

### Removed
* Removed SessionFactorySupplierFactory.

## [2.0.3] - 2017-09-25 [YANKED]

## [2.0.2] - 2017-08-01
### Added
* Flag to prevent flushing the federation configuration out when the server stops.

### Changed
* Configure Maven deploy plug-in.

## [2.0.1] - 2017-07-27
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
