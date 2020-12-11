# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unpublished

### Fixed

## 5.2.25 - 2020-12-11

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

## 5.2.19 - 2020-09-17

### Added
- Added NoSQLHandleConfig.get/setMaxContentLength() to allow on-premise
configuration of a maximum request/response size. It defaults to 32MB.

### Fixed
- Added missing README.md file to release

## 5.2.17 - 2020-08-14

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
- Don't validate request sizes. On-premise only
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
cases. This prevented use of an intermediate proxy such as Nginx, which validates headers. On-premise only.
- TableUsageRequest: added validation check that startTime, endTime and limit
must not be negative value.

## 5.2.11 - 2020-02-10

### Added
- OCI Native support for the cloud service
- Include support for IAM based security in the cloud service
- When using the cloud service tables are now created in compartments.  Compartments can be specified for tables in APIs and query statements. By default the compartment is the root compartment of the tenancy when authenticated as a specific user. The compartment name or id can be specified by default in NoSQLHandleConfig or specified in each Request object. The compartment name can also be used a prefix on a table name where table names are accepted and in queries, e.g. "mycompartment:mytable".

### Changed
- Deprecated use of TableResult.waitForState methods in favor of the simpler, not static, TableResult.waitForCompletion method.

### Removed
- Removed support for IDCS based security in the cloud service

## 5.1.15 - 2019-11-18

### Changed
- Enabled SSL hostname verification
- Reduced logging severityof bad http channels
- Bundle newer versions of netty and Jackson libraries

## 5.1.12 - 2019-08-20
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
- Added OperationNotSupportedException to handle operations that are specific to on-premise and cloud service environments

- Support for both the Oracle NoSQL Database Cloud Service and the on-premise Oracle NoSQL Database product.
  - Added StoreAccessTokenProvider for authentication of access to an on-premise store
  - Added AuthenticationException to encapsulate authentication problems when
  accessing an on-premise store.
  - Added SystemRequest, SystemStatusRequest, and SystemResult for administrative
  operations that are not table-specific.
  - Added NoSQLHandle.doSystemRequest to encapsulate a SystemRequest and waiting for its completion in a single, synchronous call.
  -   Now that the driver can access both the cloud service and an on-premise store
  some operations, classes and exceptions are specific to each environment. These are
  noted in updated javadoc.

## 18.277 - 2018-10-04
This was the initial release of the Java driver for the Oracle NoSQL Database
Cloud Service.
