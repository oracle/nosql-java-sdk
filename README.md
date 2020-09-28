# Oracle NoSQL SDK for Java

## Requirements

Java versions 8 and higher are supported.

## Overview

The Oracle NoSQL SDK for Java provides interfaces,
documentation, and examples to help develop Java
applications that connect to the Oracle NoSQL
Database Cloud Service, Oracle NoSQL Database or to the Oracle NoSQL
Cloud Simulator (which runs on a local machine). In order to
run the Oracle NoSQL Cloud Simulator, a separate download is
necessary from the Oracle NoSQL OTN download page. The Oracle NoSQL
Database Cloud Service and Cloud Simulator are referred to as the "cloud
service" while the Oracle NoSQL Database is referred to as "on-premise."

The API for all environments is the same, with the exception of some
environment-specific classes and methods, mostly related to authentication
and authorization. The API documentation clearly notes environment-specific
information.

See also:

* [Oracle NoSQL Database Cloud Service](https://docs.oracle.com/en/cloud/paas/nosql-cloud/nosql_dev.html)
* [Oracle NoSQL Database](https://www.oracle.com/database/technologies/related/nosql.html)

## Examples

Several example programs are provided in the examples directory to
illustrate the API. These examples can be run against the Oracle NoSQL
Database, the NoSQL Database Cloud Service or an instance of the Oracle
NoSQL Cloud Simulator. The code that differentiates among the configurations
is in the file Common.java and can be examined to understand the differences.

### Connecting to the Oracle NoSQL Database Cloud Service

You will need an Oracle Cloud account and credentials to use this SDK. With this
information, you'll set up a client configuration to tell your application how to
find the cloud service, and how to properly authenticate.
See [Acquring Credentials](https://www.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud/csnsd&id=acquire-creds)
for details of how to get credentials. This only needs to be done once for any
user.

You should have the following information in hand:

1. Tenancy OCID
2. User OCID
3. Public key fingerprint
4. Private key file
5. Optional private key pass phrase

Set Up Example Credentials. Credentials can be provided directly in API or in a
configuration file. The default configuration in examples/Common.java uses
a configuration file in ~/.oci.config with the following contents:

    [DEFAULT]
    tenancy=<User OCID>
    user=<Tenancy OCID>
    fingerprint=<Public key fingerprint>
    key_file=<PEM private key file>
    pass_phrase=<Private key passphrase>

See instructions in examples/Common.java for details.

### Connecting to the Oracle NoSQL Database Cloud Simulator

When you develop an application, you may wish to start with
[Oracle NoSQL Database Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/develop-oracle-nosql-cloud-simulator.html).
The Cloud Simulator simulates the cloud service and lets you write and test
applications locally without accessing the Oracle NoSQL Database Cloud Service.
You may run the Cloud Simulator on localhost.

### Connecting to the Oracle NoSQL Database On-premise

The on-premise configuration requires a running instance of Oracle NoSQL
Database. In addition a running proxy service is required. See
[Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads, and see
[Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/20.2/admin/proxy-and-driver.html)
for proxy configuration information.

### Compile and Run Examples

Compile Examples:

    $ cd examples
    $ javac -cp ../lib/nosqldriver.jar *.java


#### Run against the Oracle NoSQL Database Cloud Service

    $ java -cp .:../lib/nosqldriver.jar BasicTableExample <region>
        e.g.
    $ java -cp .:../lib/nosqldriver.jar BasicTableExample us-ashburn-1

The region argument will change depending on which region you use.

#### Run against the Oracle NoSQL Database Cloud Simulator

Run against the Oracle NoSQL Cloud Simulator using its default endpoint
of localhost:8080, assuming that the Cloud Simulator has been started. If
started on a different host or port adjust the endpoint accordingly.

    $ java -cp .:../lib/nosqldriver.jar BasicTableExample localhost:8080

#### Run against the Oracle NoSQL Database On-premise

Running against the on-premise Oracle NoSQL Database on-premise requires
a running instance of the database and running proxy service. See above.

Run against a not-secure proxy and store, with the proxy running on port 80:

    $ java -cp .:../lib/nosqldriver.jar BasicTableExample http://localhost:80 -useKVProxy

When using a secure proxy and store the proxy will generally run on port 443 and
requires SSL configuration. In addition the store requires a valid user and
password which must have been created via administrative procedures.

Assumptions for this command:

1. a driver.trust file in the current directory with the password "123456"
2. user "driver" with password "Driver.User@01". This user must have been created
in the store and must have permission to create and use tables.

Run the command:

    $ java -Djavax.net.ssl.trustStorePassword=123456 \
         -Djavax.net.ssl.trustStore=driver.trust -cp .:../lib/nosqldriver.jar \
         BasicTableExample https://localhost:443 -useKVProxy -user driver \
        -password Driver.User@01

## Licenses

See the [LICENSE](LICENSE.txt) file.

The following libraries in this download are Oracle
libraries and are under the OTN developer license, as per the download:
nosqldriver.jar

The [THIRD\_PARTY\_LICENSES](THIRD_PARTY_LICENSES.txt) file contains third
party notices and licenses.

## Documentation

API documentation is contained in the doc directory.


## Help

* Post your question on the [Oracle NoSQL Database Community](https://community.oracle.com/community/groundbreakers/database/nosql_database).
* [Email to nosql\_sdk\_help\_grp@oracle.com](mailto:nosql_sdk_help_grp@oracle.com)

When requesting help please be sure to include as much detail as possible,
including version of the SDK and **simple**, standalone example code as needed.
