# Change Log
All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unreleased

### Added
- Cloud only: added new OCI regions (TYO, AHU, DAC, DOH, IZQ, UKY, XSP)
- Cloud only: added use of ETag in AddReplicaRequest and DropReplicaRequest
- Cloud only: added OKE workload identity authentication support
 - SignatureProvider.createWithOkeWorkloadIdentity()
 - SignatureProvider.createWithOkeWorkloadIdentity(String serviceAccountToken, Logger logger)
 - SignatureProvider.createWithOkeWorkloadIdentity(File serviceAccountTokenFile, Logger logger)

### Fixed
- Changed handle close to not use the Netty "quiet period" when shutting down
  the worker group. This makes handle close much faster as well as making it
  faster to acquire authentication information in the cloud service when using a
  method that uses an HTTP request such as Instance Principal authentication

## [5.4.14] 2023-11-17

### Added
- Added SDK version number in exception messages

### Changed
- Modified internal query processing to better support elasticity operations
- Update netty dependency to 4.1.101.Final


## [5.4.13] 2023-09-25

### Added
- added support for array_collect() and count(distinct) in queries. These
  require server side support which is only available in Oracle NoSQL releases
  23.3 and higher and will not be immediately available in the cloud service

- Cloud only: added support for replica (multi-region) tables
  - Added new methods to NoSQLHandle
    - NoSQLHandle.addReplica()
    - NoSQLHandle.dropReplica()
    - NoSQLHandle.getReplicaStats()
  - Added new requests and result
    - AddReplicaRequest
    - DropReplicaRequest
    - ReplicaStatsRequest, ReplicaStatsResult
  - Added new information to TableResult
    - isFrozen()
    - isReplicated()
    - isLocalReplicaInitialized()
    - getReplicas()
- Cloud only: added new OCI regions (BOG, OZZ, DRS)

## [5.4.12] 2023-07-28

### Added
- Cloud only: added session token authentication support
  - SignatureProvider.createWithSessionToken()
  - SignatureProvider.createWithSessionToken(String profile)
  - SignatureProvider.createWithSessionToken(String configFilePath, String profile)
- Cloud only: added support to read region from system environment variable
  OCI_REGION if using user principal or session token authentication
- Cloud only: added new OCI regions (NAP, AVZ, AGA, VAP)

### Changed
- Moved a couple internal log messages to FINE instead of INFO
- Cleaned up messaging when can't connect to server
- Modified internal auth logic to avoid "inactive channel: retrying" messages

## [5.4.11] 2023-06-06

### Fixed
- Changed internal IAM logic to close channel on SSL errors before doing internal retry

## [5.4.10] 2023-04-25

### Added
- On-premises only: added support for setting namespace on a per-request basis
- Cloud only: added new OCI regions (MTY, STR, BEG, VLL, YUM)

### Changed
- Allow a space in addition to a "T" to separate date and time in String
 TimestampValue format
- Code cleanup, removing unused/obsolete code and tests

### Fixed
- Internal: changed to use nanoTime() instead of currentTimeMillis() to avoid
 possible issue if system clock rolls backwards
- Additional internal per-request-iteration timeout corrections

## [5.4.9] 2023-02-14

### Added
- On-premises only: added support for default namespace in NoSQLHandleConfig

### Changed
- Updated javadoc for QueryRequest try-with-resources

### Fixed
- Fixed timeout value sent to server on internal retries

## [5.4.8] 2023-01-05

### Fixed
- Cloud only: Fixed an issue where a long running application using SignatureProvider
 with instance principal may encounter NotAuthenticated error after several minutes even
 if authentication succeeds for the first requests.

### Changed
- Updated copyrights to 2023
- Update netty dependency to 4.1.86.Final

## [5.4.7] 2022-12-06

Note: there are no 5.4 releases before 5.4.7

### Added
- Support for new, flexible wire protocol (V4) has been added. The previous protocol
is still supported for communication with servers that do not yet support V4. The
version negotation is internal and automatic; however, use of V4 features will fail
at runtime when attempted with an older server. Failure may be an empty or
undefined result or an exception if the request cannot be serviced at all. The following
new features or interfaces depend on the new protocol version
 - added set/getDurability to QueryRequest for queries that modify data
 - added pagination information to TableUsageResult and TableUsageRequest
 - added shard percent usage information to TableUsageResult
 - added IndexInfo.getFieldTypes to return the type information on an index on a JSON
field
 - added the ability to ask for and receive the schema of a query using
     * PrepareRequest.setGetQuerySchema
     * PreparedStatement.getQuerySchema
 - Cloud only: added use of ETags, DefinedTags and FreeFormTags in TableRequest and TableResult
 - Cloud only: Updated OCI regions (SGU, IFP)

### Changed
- Consistency is now a class and no longer a simple enumeration. Applications must
be recompiled but source compatibility is maintained for all but the more complex
use of an enumeration
- Made one private serializer class public to allow for improved internal testing.
- MapValue now implements Iterable

## [5.3.7] 2022-10-18

### Changed
- Allow application to retry a QueryRequest if it gets a timeout exception and the query only does reads
- Cloud only: Updated OCI regions (ORD, BGY, TIW, MXP, DUS, DTM, ORK, SNN)
- Update netty dependency to 4.1.82.Final

### Fixed
- Cloud only: Fixed an issue where a long running application using SignatureProvider
 with resource principal may encounter NotAuthenticated error after several minutes even
 if authentication succeeds for the first requests.

## [5.3.6] 2022-08-23

### Added
- Added support for parent/child tables usage in WriteMultiple requests.

## [5.3.5] 2022-07-20

### Added
- Made one private serializer class public to allow for improved internal testing.

## [5.3.4] 2022-06-16

NOTE: there was briefly a 5.3.3 release available on GitHub. This release is functionally
identical. It just adds license files to the jar artifacts

### Added
- added new method in NoSQLHandle queryIterable to return the results of a
query in an iterable/iterator format. The returned QueryIterableResult should
be used in a try-with-resources statement to ensure proper closing of
resources.
- updated NoSQLHandle and QueryRequest interfaces to extend AutoClosable
- added Version.createVersion(byte[]) to allow creation of a Version object from a
query that returns row_version() as a BinaryValue. The Version can be used for
conditional put and delete operations
- added support for setting an extension to the User Agent http header by
setting the ExtensionUserAgent property on NoSQLHandlerConfig.
- Cloud only: Added OCI regions: CDG (Paris), MAD (Madrid), QRO (Queretaro)

## [5.3.2] 2022-03-21

### Added
- new methods in NoSQLHandleConfig to control keeping a minimum number of connections alive in the connection pool
 - get/setConnectionPoolMinSize
- Added new methods in NoSQLHandleConfig to control the SSL handshake timeout,
default 3000 milliseconds.
  - get/setSslHandshakeTimeout

### Deprecated
- several methods in NoSQLHandleConfig are deprecated as they have no effect on the new connection pool implementation. The methods remain, temporarily, but they do not control the pool and have no effect
 - get/setConnectionPoolSize
 - get/setPoolMaxPending

### Changed
- there is a new connection pool implementation to simplify management of connections by reducing the need for configuration. The new pool will create new connections on demand, with no maximum.  It will also remove them to the configured minimum (default 0) after a period of inactivity
- the new connection pool will send periodic keep-alive style HTTP requests to maintain activity level. These requests are only sent if a minimum size is configured using NoSQLHandleConfig.setConnectionPoolMinSize. This mechanism is mostly useful in the cloud service because the server side will close idle connections. Creating new connections requires a new SSL handshake and can add latency to requests after idle time.

### Fixed
- Cloud only: Fixed an issue that a request may have unexpected latency when
authenticated with instance/resource principal due to security token refresh.

## [5.3.1] 2022-02-17

See also sections on 5.2.30 and 5.2.31 as they were not released publicly. The last public release was 5.2.29.

### Added
- Cloud only: Support for On-Demand tables in TableLimits
- Row modification time made available in GetResult
- Existing row modification time made available in PutResult and DeleteResult when operation fails and previous value is requested
- On-Prem only: Support for setting Durability in write operations (put/delete/writeMultiple/multiDelete)

### Changed
- Internally, the SDK now detects the serial version of the server it's connected to, and adjusts its capabilities to match. If the server is an older version, and some features may not be available, client apps may get a one-time log message (at INFO level) with text like "The requested feature is not supported by the connected server".


## [5.2.31] 2022-01-28

Oracle internal use release

### Changed
- Updated copyrights to 2022
- Internal changes to handling of channel (connection) acquisition, including
addition of retries, within the request timeout period.
- Enhanced logging of timeout and connection-related exceptions

### Fixed
- Cloud only: Updated internal rate limiter processing to work
properly with Instance Principals

## [5.2.30] 2022-01-21

Oracle internal use release

### Changed
- Cloud only: Updated OCI regions

### Added
- Added client statistics. Users can enable internal driver statistics by
using system property -Dcom.oracle.nosql.sdk.nosqldriver.stats.profile=
[none|regular|more|all] or by using the NoSQLHandleConfig.setStatsProfile() api.
- Added StatsControl interface as well as methods to control stats in
NoSQLHandleConfig and NoSQLHandle.
- Added to NoSQLHandleConfig:
 - get/setStatsInterval
 - get/setStatsProfile
 - get/setStatsPrettyPrint
 - getStatsHandler
- Added to NoSQLHandle:
 - getStatsControl

## [5.2.29] 2021-11-18

NOTE: version number 5.2.28 has been skipped

### Fixed
- Fixed NoSQLHandleConfig.getLibraryVersion(), which has been returning null
for a few releases.
- Cloud only:
  - Updated OCI regions
  - Fixed an issue using instance principal in OC2 realm. A SignatureProvider created
  with an instance principal might fail to obtain a security token from IAM and get
  "400 Bad Request" without any additional message.
- Fixed default logging so that its level can be better affected by a logging
configuration file

### Changed

The default networking configuration has changed to better scale without user
configuration and to work better with the underlying implementation.

- The default number of threads used for network traffic has changed from 2 to the
number of CPUs available * 2. This also affects the behavior of
NoSQLHandleConfig.setNumThreads() and NoSQLHandleConfig.getNumThreads()
- The default number of connections in the pool has changed from 2 to the
number of CPUs available * 2. This also affects the behavior of
NoSQLHandleConfig.setConnectionPoolSize() and
NoSQLHandleConfig.getConnectionPoolSize()
- The default number of pending calls to acquire a connection in the pool has
changed from 6 to 3. This also affects the behavior of
NoSQLHandleConfig.setPoolMaxPending() and NoSQLHandleConfig.getPoolMaxPending().
- Update netty dependency to 4.1.68.Final
- Update Jackson dependency to 2.12.4

## [5.2.27] - 2021-05-13

### Added
- Cloud only:
  - Added new SignatureProvider constructors to allow use of an instance principal with delegation token in a file for authorization and authentication.
    - SignatureProvider.createInstancePrincipalForDelegation(File delegationTokenFile)
    - SignatureProvider.createInstancePrincipalForDelegation(String iamAuthUri, Region region, File delegationTokenFile, Logger logger)
- Added methods on FieldValue for convenience checking of whether an instance is
of a given type, e.g. FieldValue.isInteger(), etc.

### Changed
- Cloud only:
  - Use SHA-256 to generate the fingerprint of instance principal certificate to
 request IAM security token.
 - The atomic value access methods in FieldValue such as getInt(), getLong(), etc will
 now succeed for all type conversions that do not lose information, doing implicit
 coercion. Previously they would throw ClassCastException if the FieldValue was not
 of the specific type.
 - Some methods that would throw NullPointerException for missing or
 invalid configurations now throw IllegalArgumentException. Related messages have
 been clarified.
 - Changed default duration of signature cache of SignatureProvider from 5 to 4 minutes
 - NoSQLHandle creation now performs SignatureProvider warm-up that will pre-create
 and cache the signature. Errors or delays occur during creating signature may
 result in handle creation failure that would not have happened in previous releases (cloud service only)

### Fixed
- Use correct netty constructor when using an HTTP proxy without a username or
password
- Fixed a problem where the cloud service might succeed when dropping a table
that does not exist without using "drop table if exists" when it should throw
TableNotFoundException
- Fixed javadoc for NoSQLHandleConfig.setTableRequestTimeout to remove incorrect statement
- Unhide NoSQLHandleConfig methods to deal with an HTTP proxy
 - setProxyHost
 - setProxyPort
 - setProxyUsername
 - setProxyPassword
- Cloud service only, instance principal issues:
  - Fixed an issue where the first request issued using instance principal may take
 longer than 5 seconds and throw RequestTimeoutException if handle uses the default
 request timeout.
  - Fixed a problem where SignatureProvider with instance principal may use expired
 security token issued by IAM to create signature, which causes InvalidAuthorizationException.

## [5.2.26] - 2021-02-09

### Changed
- Updated copyrights to 2021
- Updated versions of dependencies to latest releases
- Updated links to Oracle NoSQL Database generic documentation to point to
current documentation

### Fixed
- Description for the SDK in the pom file is now correct

## [5.2.25] - 2020-12-11

### Added
- Rate Limiting (cloud only):
  - New method NoSQLHandleConfig.setRateLimitingEnabled() to enable automatic internal rate limiting based on table read and write throughput limits.
  - If rate limiting is enabled:
    - NoSQLHandleConfig.setDefaultRateLimitingPercentage() can control how much of a table's full limits this client handle can consume (default = 100%).
    - Result classes now have a getRateLimitDelayedMs() method to return the amount of time an operation was delayed due to internal rate limiting.
  - Alternately, rate limiters can be supplied on a per-request basis.
  - For more information, see RateLimitingExample.java and the java docs for RateLimiterConfig.
- RetryStats: New object allows the application to see how much time and for what reasons an operation was internally retried.
  - For successful operations, retry stats can be retrieved using Result.getRetryStats().
  - Otherwise, the original Request may have retry stats available via Request.getRetryStats() (for example, after an exception was thrown).
- Cloud only: New regions: ap-chiyoda-1, me-dubai-1, uk-cardiff-1 and sa-santiago-1
- Cloud only: Added new SignatureProvider constructors to allow use of an instance
principal with a delegation token for authorization and authentication:
 - SignatureProvider.createInstancePrincipalForDelegation()
 - BinaryValue constructor to create BinaryValue from a Base64-encoded string

### Fixed
- Ensure that TableLimits is always null in TableResult on-premise.
- Fixed a problem where SignatureProvider.getAuthorizationString may fail due to an IllegalStateException with error "Timer already cancelled".
- Add timezone offset to the string representation of a TimestampValue to properly conform to ISO 8601 format.
- Fixed request timeout in README.md quickstart example. 60s would cause
problems with some environments
- Cloud only. Fixed issue where a handle wouldn't fully close because of a lingering
thread, interfering with process exit

### Changed
- DefaultRetryHandler now uses incremental backoff mechanism (instead of fixed 1-second delay) and may be extended.
- Updated examples to use doTableRequest() instead of tableRequest() followed by waitForCompletion().

## [5.2.19] - 2020-09-17

### Added
- Added NoSQLHandleConfig.get/setMaxContentLength() to allow on-premise
configuration of a maximum request/response size. It defaults to 32MB.

### Fixed
- Added missing README.md file to release

## [5.2.17] - 2020-08-14

### Added
- Added NoSQLHandleConfig.setSSLProtocols() to allow the user to configure preferred SSL protocol
- Cloud only. Added the support in SignatureProvider to configure and pass region to NoSQLHandleConfig using new constructor of NoSQLHandleConfig(AuthorizationProvider):
  - SignatureProvider built with OCI standard config file is now able to read 'region' parameter from config file and pass to NoSQLHandleConfig implicitly
  - Added a new constructor SignatureProvider(String tenantId, String userId, String fingerprint, File privateKeyFile, char[] passphrase, Region region) to allow passing Region programmatically with user profile
  - Added two new builder methods SignatureProvider.createWithInstancePrincipal(Region region) and SignatureProvider.createWithInstancePrincipal(String iamAuthUri, Region region, Logger logger) to allow setting Region with instance principal
- Cloud only. Added new regions: AP_MELBOURNE_1, AP_OSAKA_1, ME_JEDDAH_1, EU_AMSTERDAM_1, CA_MONTREAL_1
- Added static JsonOptions object, PRETTY, for convenience
- Added generic group by and SELECT DISTINCT. These features will only work with servers that also support generic group by.
- Added a new class, JsonReader, to enable construction of multiple MapValue instances from a stream of JSON objects from a String, File, or InputStream
- Cloud only. Added support for authenticating via Resource Principal. This can be used in Oracle Cloud Functions to access NoSQL cloud service
  - Added two new builder methods SignatureProvider.createWithResourcePrincipal and SignatureProvider.createWithResourcePrincipal(Logger logger)
  - Added a new method SignatureProvider.getResourcePrincipalClaim(String key) to retrieve resource principal metadata with ResourcePrincipalClaimKeys such as compartment and tenancy OCID
- Added JsonOptions.setMaintainInsertionOrder() which, if set, will cause
MapValue  instances created from JSON to maintain the insertion order of fields
in the map so that iteration is predictable.

### Fixed
- Don't validate request sizes. On-premises only
- JsonOptions.setPrettyPrint(true) now works
- Request timeouts now operate correctly on milliseconds instead of rounding up to seconds
- Changed min/max implementation to make them deterministic
- TableUsageRequest: added validation check that endTime must be greater than
startTime if both of them are specified, throw IAE if endTime is smaller than
startTime.
- Fix a possible memory leak that could occur if an error was returned before
a request was sent.
- Fixed out of order parameters in PreparedStatement.copyStatement
- Deprecated the TableBusyException, which will no longer occur. It will be
removed in a future version.
- Fix another memory leak that could occur on the receive side when the response
was discarded due to unmatched request Id.
- Fixed a problem where the HTTP Host header was not being adding in all request
cases. This prevented use of an intermediate proxy such as Nginx, which validates headers. On-premises only.
- TableUsageRequest: added validation check that startTime, endTime and limit
must not be negative value.

## [5.2.11] - 2020-02-10

### Added
- OCI Native support for the cloud service
- Include support for IAM based security in the cloud service
- When using the cloud service tables are now created in compartments.  Compartments can be specified for tables in APIs and query statements. By default the compartment is the root compartment of the tenancy when authenticated as a specific user. The compartment name or id can be specified by default in NoSQLHandleConfig or specified in each Request object. The compartment name can also be used a prefix on a table name where table names are accepted and in queries, e.g. "mycompartment:mytable".

### Changed
- Deprecated use of TableResult.waitForState methods in favor of the simpler, not static, TableResult.waitForCompletion method.

### Removed
- Removed support for IDCS based security in the cloud service

## [5.1.15] - 2019-11-18

### Changed
- Enabled SSL hostname verification
- Reduced logging severityof bad http channels
- Bundle newer versions of netty and Jackson libraries

## [5.1.12] - 2019-08-20
### Fixed
- Modified MapValue and handling of GetResult and QueryResult to maintain declaration order for fields. For queries, this is the order in which they are selected. For rows it is the declaration order from the initial table creation.

### Changed
- Changed version numbering system to major.minor.patch, starting with 5.1.x
- Change default logging level to WARNING

### Added
- Added new constructor for NoSQLHandleConfig to take a string endpoint. It should be used in preference to the constructor that accepts a URL.
- Added PutRequest.setExactMatch() to allow the user to control whether an
exact schema match is required on a put. The default behavior is false.
- Support for complex, multi-shard queries:
  - Sorted/ordered multi-shard queries.
  - Multi-shard aggregation.
  - Geo-spatial queries such as geo_near().
- Support for Identity Columns:
  - Added PutRequest.get/setIdentityCacheSize() to allow a user to control the number of cached values are used for identity columns. The default value is set when the identity column is defined.
  - Added PutResult.getGeneratedValue() which will return a non-null value if an identity column value was generated by the operation. This is only relevant for tables with an identity column defined.
- Added a new, simpler TableResult.waitForCompletion() interface to wait for the completion of a TableRequest vs waiting for a specific state.
- Added NoSQLHandle.doTableRequest to encapsulate a TableRequest and waiting for
  its completion in a single, synchronous call.
- Added OperationNotSupportedException to handle operations that are specific to on-premises and cloud service environments

- Support for both the Oracle NoSQL Database Cloud Service and the on-premises Oracle NoSQL Database product.
  - Added StoreAccessTokenProvider for authentication of access to an on-premises store
  - Added AuthenticationException to encapsulate authentication problems when
  accessing an on-premises store.
  - Added SystemRequest, SystemStatusRequest, and SystemResult for administrative
  operations that are not table-specific.
  - Added NoSQLHandle.doSystemRequest to encapsulate a SystemRequest and waiting for its completion in a single, synchronous call.
  -   Now that the driver can access both the cloud service and an on-premises store
  some operations, classes and exceptions are specific to each environment. These are
  noted in updated javadoc.

## [18.277] - 2018-10-04
This was the initial release of the Java driver for the Oracle NoSQL Database
Cloud Service.
