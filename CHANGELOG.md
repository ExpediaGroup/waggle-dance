
## [4.0.1] - 2025-06-27
### Fix
- Fixed issue where a call to an unavailable metastore got redirected to "primary" which results in NoSuchObjectException instead of TException causing clients to not retry. 
- Fixed unnecessary function prefix on parameterized functions. [#344](https://github.com/ExpediaGroup/waggle-dance/pull/344)
- Fixed unnecessary function prefix. [#343](https://github.com/ExpediaGroup/waggle-dance/pull/343)
- Fixed pattern processing to federated metastore. [#342](https://github.com/ExpediaGroup/waggle-dance/pull/342)
- Removed call to primary and made it only when Kerberos (SASL) is enabled. 
- Fixed Handler creation for SASL.
- Add HiveConf cache to `CloseableThriftHiveMetastoreIfaceClientFactory` to prevent threads block. See [#325](https://github.com/ExpediaGroup/waggle-dance/issues/325)
- Fix for `NullPointerException` in ClientCapabilities when `get_table_req` and `get_tables_req` is called from Hive3.x client. See [#317](https://github.com/ExpediaGroup/waggle-dance/issues/317)
### Added
- Metric for monitoring open transports. `<prefix>.com_hotels_bdp_waggledance_open_transports_gauge`
### Changed
- Removed RetryingHMSHandler. Retries are done in the client there should be no need to wrap everything in retry logic again. 

## [4.0.0] - 2025-02-24
### Changed (Backward incompatible)
* Hive 3, waggle dance 4.x.x will be Hive 3 based for Hive 2 use waggledance 3.x.x releases.

### Fixed
* Issue where the primary prefix when creating database. https://github.com/ExpediaGroup/waggle-dance/pull/336
* Various fixes and support for Kerberos was contributed.

### Updated Dependency
* `hive` updated to `3.1.3` (was `3.1.2`).
* `hadoop` updated to `3.3.6` (was `3.1.0`).
* `spring` updated to `2.7.13` (was `2.0.4-RELEASE`).
* `guava` updated to `31.1-jre` (was `23.0`).
* `guice` updated to `4.13.2` (was `4.13.1`).
* `junit` updated to `5.1.0` (was `4.0`).
* `aspectj` updated to `1.9.7` (was `1.8.9`).
* `hcommon-hive-metastore` updated to `1.4.2` (was `1.2.3`).

### Newly Added Dependency
* `lombok` - `1.18.24`.
* `jakarta` - `6.0.0`.
* `apache-commons` - `3.12.0`.
  
## [3.13.0] - 2024-04-19
### Added
- Added `waggle-dance-extensions` module. See [extensions README](waggle-dance-extensions/README.md.)
- Added support to enable Rate Limiting in Waggle Dance.
### Changed
- Changed and added some log messages for better tracking of calls.
- Changed Integration Test WaggleDanceRunner to allow for reuse.

## [3.12.0] - 2024-02-08
### Added
- Added optional `primary-meta-store.read-only-remote-meta-store-uris` config to allow traffic to be diverted based on calls made. See README.md.

## [3.11.7] - 2023-11-30
### Changed
- Fixed log statement that was not logging the exception correctly.
- Removed client.shutdown() call, this always throws an exception and the code ends up closing the transport directly.

## [3.11.6] - 2023-10-24
### Fixed
- Added lombok
- Fixed test cases
- Fixed issue where the primary metastore was not applying the allow filter to validate database clashes from other metastores.
- Switch to ExecutorService instead of the default `ForkJoinPool` for `MetastoreMappingImpl.isAvailable()` calls. Using `ForkJoinPool` may cause threads to wait on each other.
- Increased default `MetastoreMappingImpl.isAvailable()` timeout to `2000ms` (was `500ms`) to set a bit more conservative default.

## [3.11.5] - 2023-10-23
### Fixed
- Added timeout on `MetastoreMappingImpl.isAvailable()` calls to prevent long waits on unresponsive metastores.

## [3.11.4] - 2023-08-23
### Changed
- Metrics have been incorporated into Waggle Dance with the inclusion of tags, which will facilitate filtering within Datadog.
- Exclude jetty-all from core module, because it makes spring start fail and makes `WaggleDanceIntegrationTest` fail.
- Upgrade maven.surefire.plugin.version to 3.1.2 (was 3.0.0-m5).
- Exclude jdk.tools clashes with > java8 JDK.
### Fixed
- Exclude Junit5 dependencies as they clashed with Junit4 and caused maven to stop running the tests.
- Fixed metric(graphite) integration test (was broken since 3.10.12 (spring-boot upgrade).

## [3.11.3] - YANKED
### Fixed
- Exclude Junit5 dependencies as they clashed with Junit4 and caused maven to stop running the tests.
- Fixed metric(graphite) integration test (was broken since 3.10.12 (spring-boot upgrade).
### Changed
- Exclude jetty-all from core module, because it makes spring start fail and makes `WaggleDanceIntegrationTest` fail.
- Upgrade maven.surefire.plugin.version to 3.1.2 (was 3.0.0-m5).
- Exclude jdk.tools clashes with > java8 JDK.

## [3.11.2] - 2023-07-04
### Changed
- Setting AWSGlueClientFactory log level to `WARN` because it spams this [log](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/aws-glue-datacatalog-client-common/src/main/java/com/amazonaws/glue/catalog/metastore/AWSGlueClientFactory.java#L57) every ~200ms. It could be creating unnecessary Glue clients.

## [3.11.1] - 2023-05-31
### Fixed
- Clean up delegation-token set for Kerberos in thread-local.

## [3.11.0] - 2023-05-22
### Fixed
- Support kerberos and delegation-token See [#264](https://github.com/ExpediaGroup/waggle-dance/issues/264)
### Changed
- Upgrade version of snakeyaml to 1.32 (was 1.26)

## [3.10.14] - 2023-05-11
### Changed
- Remove `waggledance.allow-bean-definition-overriding` property to configuration to favor single bean creation.

## [3.10.13] - Not released [YANKED]

## [3.10.12] - 2023-05-04 - [YANKED]
### Changed
- Upgraded `springboot` version to `2.7.11` (was `2.0.4.RELEASE`).
- Added `spring-boot-starter-validation`.
- Added `waggledance.allow-bean-definition-overriding` property to configuration.
- Added `joda-time` version `2.9.9`.

## [3.10.11] - 2023-02-06
### Added
- Functionality to get tables from a database using a Glue federation. Code pulled from original AWS master branch.
- [Code](https://github.com/ExpediaGroup/aws-glue-data-catalog-client-for-apache-hive-metastore/commit/7f8f13681b09d07dafb57e6efdae457a5c6f6d7b)

## [3.10.10] - 2022-12-01
### Changed     
- Upgraded `aws-sdk` version to `1.12.276` (was `1.11.267`) in `waggledance-core`.
- Enabled support to use AWS STS tokens when using Glue sync in `waggledance-core`.

## [3.10.9] - 2022-11-29
### Changed     
- Uploaded Glue JARs with all changes from release `3.10.8` in `/lib` folder.
- Excluded `pentaho-aggdesigner-algorithm` dependency from `hive-exec` (provided) due to problems when building the project locally.

## [3.10.8] - 2022-11-24
### Changed     
- Upgraded `aws-sdk` version to `1.12.276` (was `1.11.267`) in `aws-glue-datacatalog-client-common`.
- Enabled support to use AWS STS tokens when using Glue sync in `aws-glue-datacatalog-client-common`.

## [3.10.7] - 2022-09-02
### Fixed     
- Fixed get objectname null pointer for:
    - `transformInboundHiveObjectRef`
    
## [3.10.6] - 2022-06-07
### Fixed
- Fixed database name translation for:
  - `alter_partitions_with_environment_context`
  - `alter_table_with_cascade`

## [3.10.5] - 2022-05-23
### Changed
* Added `queryFunctionsAcrossAllMetastores` configuration for optimising `getAllFunctions` calls.
### Added
* Metrics to track metastore availability. 

## [3.10.4] - 2022-04-17
### Fixes
* More tuning of delayed `set_ugi` calls.

## [3.10.3] - 2022-04-16
### Fixes
* Potential exception when `set_ugi` has immutable list or null-value groups argument.

## [3.10.2] - 2022-04-19
### Changed
* Caching `set_ugi` call in clients to prevent unnecessary calls to metastores.

## [3.10.1] - 2022-04-06
### Added
* Converted `metastore.isAvailable` loops to parallel execution to mitigate slow responding metastores.

## [3.10.0] - 2022-03-01
### Changed
* Support for Glue catalog (read only) federation.
* converted some log statements to debug to get less chatty logs.

## [3.9.9] - 2022-01-19
### Changed
* `log4j2` updated to `2.17.1` (was `2.17.0`) - log4shell vulnerability fix

## [3.9.8] - 2021-12-20
### Changed
* `log4j2` updated to `2.17.0` (was `2.16.0`) - log4j vulnerability fix see https://logging.apache.org/log4j/2.x/security.html

## [3.9.7] - 2021-12-14
### Changed
* `log4j2` updated to `2.16.0` (was `2.15.0`) - log4shell vulnerability fix

## [3.9.6] - 2021-12-14
### Changed
* `log4j2` updated to `2.15.0` (was `2.10.0`) - log4shell vulnerability fix

## [3.9.5] - 2021-08-23
### Changed
* `commons-io` updated to `2.7.` (was `2.6`).
* `org.pentaho:pentaho-aggdesigner-algorithm` dependency excluded from `waggle-dance-core`.

## [3.9.4] - 2021-04-08
### Fixed
* Support for '.' wildcards in database pattern calls. See [#216](https://github.com/HotelsDotCom/waggle-dance/issues/216)

## [3.9.3] - 2021-03-15
### Fixed
* Null Pointer Exception when database name was null in `get_privilege_set` call.

## [3.9.2] - 2021-03-12
### Fixed
* Changed spring-boot-maven-plugin layout to ZIP (was JAR). This fixes classloading issues with external jars (which can be loaded by adding `-Dloader.path=my.jar`).

## [3.9.1] - 2021-03-04
### Fixed
* Null pointer exception when creating a metastore tunnel by adding a check for null `configuration-properties`.
* Fixing issue where Presto views cannot be parsed resulting in errors.

## [3.9.0] - 2021-02-26
### Added
* Support for setting Hive metastore filter hooks which can be configured per federated metastore. See the [README](https://github.com/HotelsDotCom/waggle-dance#federation) for more information.
### Fixed
* The `configuration-properties` from `waggle-dance-server.yml` are set when creating the Thrift clients.

## [3.8.0] - 2020-11-25
### Added
* New `mapped-tables` feature. See [#195](https://github.com/HotelsDotCom/waggle-dance/issues/195) and the [README](https://github.com/HotelsDotCom/waggle-dance#federation) for more information.

### Changed
* Updated `hotels-oss-parent` to 6.1.0 (was 5.0.0).

## [3.7.0] - 2020-09-16
### Changed
* Upgraded version of `hive.version` to `2.3.7` (was `2.3.3`). Allows Waggle Dance to be used on JDK>=9.

### Added
* Implemented `get_partition_values()` method in `FederatedHMSHandler` due to Hive version change.
* New `database-name-mapping` feature. See the [README](https://github.com/HotelsDotCom/waggle-dance#database-name-mapping) for more information.
### Changed
* Removed `IdentityMapping` as a fallback mapping in certain cases. Simplifies code paths.

## [3.6.0] - 2020-03-04
### Changed
* Updated `hotels-oss-parent` to 5.0.0 (was 4.0.1).

### Added
* Support for Prometheus metrics.

## [3.5.0] - 2019-10-14
### Added
* Added logging to help debug connection issues.

### Changed
* Remove error for empty prefix on federated metastores. See [#183](https://github.com/HotelsDotCom/waggle-dance/issues/183).

## [3.4.0] - 2019-07-14
### Added
* Support for `mapped-databases` configuration for primary metastore. See [#175](https://github.com/HotelsDotCom/waggle-dance/issues/175).

### Changed
* Removed com.hotels.bdp.waggledance.spring.CommonVFSResource, looks like dead code. See [#178](https://github.com/HotelsDotCom/waggle-dance/issues/178).

## [3.3.2] - 2019-06-25
### Changed
* Changed a prefixed *primary* metastore to fallback to 'empty prefix' if nothing specified. See [#173](https://github.com/HotelsDotCom/waggle-dance/issues/173).

## [3.3.1] - 2019-05-20
### Fixed
* `Show Functions` now shows UDFs from all metastores. See [#164](https://github.com/HotelsDotCom/waggle-dance/issues/164).
* Fixed REST API (http://localhost:18000/api/admin/federations/) which broke in 3.3.0 release.
* Prefixing of UDFs used in a view. See [#165](https://github.com/HotelsDotCom/waggle-dance/issues/165).

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
