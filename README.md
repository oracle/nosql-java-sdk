# Oracle NoSQL SDK for Java

## About

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

## Requirements

Java versions 8 and higher are supported.

## Installation

The Oracle NoSQL SDK for Java can be included in a project in 2 ways:

1. Include a dependency in a Maven project
2. Download from GitHub

### Install as a Project Dependency

This dependency can be used to include the SDK and its dependencies in your
project. The version changes with each release.

```
<dependency>
  <groupId>com.oracle.nosql.sdk</groupId>
  <artifactId>nosqldriver</artifactId>
  <version>5.2.26</version>
</dependency>
```

### Download from GitHub

You can download the Oracle NoSQL SDK for Java as an archive from
[GitHub](https://github.com/oracle/nosql-java-sdk/releases). The archive
contains the runtime library and its dependencies, examples, and
API documentation.

## Documentation

See [Oracle NoSQL SDK for Java javadoc](https://oracle.github.io/nosql-java-sdk/) for the latest API documentation.

General documentation about the Oracle NoSQL Database and the Oracle NoSQL Database Cloud Service can be found in these locations:

* [Oracle NoSQL Database Cloud Service](https://docs.oracle.com/en/cloud/paas/nosql-cloud/nosql_dev.html)
* [Oracle NoSQL Database On Premise](https://docs.oracle.com/en/database/other-databases/nosql-database/)

## Changes

See [CHANGELOG](./CHANGELOG.md) for changes in each release.

## Connect to the Oracle NoSQL Database

There are 3 environments, or services that can be used by the Oracle NoSQL
SDK for Java:

1. Oracle NoSQL Database Cloud Service
2. Oracle NoSQL Database On-premise
3. Oracle NoSQL Database Cloud Simulator

The next sections describe how to connect to each and what information is
required.

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

### Connecting to the Oracle NoSQL Database On-premise

The on-premise configuration requires a running instance of Oracle NoSQL
Database. In addition a running proxy service is required. See
[Oracle NoSQL Database Downloads](https://www.oracle.com/database/technologies/nosql-database-server-downloads.html) for downloads, and see
[Information about the proxy](https://docs.oracle.com/en/database/other-databases/nosql-database/20.3/admin/proxy-and-driver.html)
for proxy configuration information.

### Connecting to the Oracle NoSQL Database Cloud Simulator

When you develop an application, you may wish to start with
[Oracle NoSQL Database Cloud Simulator](https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/develop-oracle-nosql-cloud-simulator.html).
The Cloud Simulator simulates the cloud service and lets you write and test
applications locally without accessing the Oracle NoSQL Database Cloud Service.
You may run the Cloud Simulator on localhost.

## Quickstart

The following is a quick start tutorial to run a simple program in all supported
environments. It requires access to the Oracle NoSQL Database Cloud Service,
a running on-premise Oracle NoSQL Database instance, or a running Oracle
NoSQL Cloud Simulator instance. As a standalone program it will run most easily
using a download version of the Oracle NoSQL SDK for Java.

1. Copy this example into a local file named Quickstart.java
2. If using directly-supplied cloud service credentials edit the file, adding
credentials in the appropriate SignatureProvider constructor. The default cloud
service behavior looks for credentials in $HOME/.oci/config.
3. Compile
```
$ javac -cp <path-to-nosqldriver.jar> Quickstart.java
```
4. Run
Using the cloud service on region us-ashburn-1
```
$ java -cp .:<path-to-nosqldriver.jar> Quickstart -service cloud -endpoint us-ashburn-1
```
Using a non-secure on-premise service on endpoint http://localhost:8090
```
$ java -cp .:<path-to-nosqldriver.jar> Quickstart -service onprem -endpoint http://localhost:8090
```
Using a Cloud Simulator instance on endpoint http://localhost:8080
```
$ java -cp .:<path-to-nosqldriver.jar> Quickstart -service cloudsi -endpoint http://localhost:8080
```


```
/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import java.io.IOException;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple quickstart program to demonstrate Oracle NoSQL Database.
 * It does these things:
 * - create a table
 * - put a row
 * - get a row
 * - drop the table
 *
 * See the examples for more interesting operations. This quickstart is
 * intended to illustrate connecting to a service and performing a few
 * operations.
 *
 * This program can be run against:
 *  1. the cloud service
 *  2. the on-premise proxy and Oracle NoSQL Database instance, secure or
 *  not secure.
 *  3. the cloud simulator (CloudSim)
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar Quickstart \
 *      -service <cloud|onprem|cloudsim> -endpoint <endpoint-or-region>
 *
 * The endpoint and arguments vary with the environment.
 *
 * This quick start does not directly support a secure on-premise
 * environment. See the examples for details on that environment.
 */
public class Quickstart {

    private String endpoint;
    private String service;

    private Quickstart(String[] args) {
        /*
         * parse arguments
         */
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-service")) {
                service = args[++i];
            } else if (args[i].equals("-endpoint")) {
                endpoint = args[++i];
            } else {
                System.err.println("Unknown argument: " + args[i]);
                usage();
            }
        }
        if (service == null || endpoint == null) {
            System.err.println("-service and -endpoint are required");
            usage();
        }
    }

    private static void usage() {
        System.err.println(
            "Usage: java -cp <path-to-nosqldriver.jar> Quickstart \\ \n" +
            " -service <cloud|onprem|cloudsim> -endpoint <endpoint-or-region>");
        System.exit(1);
    }

    private NoSQLHandle getHandle() {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        configureAuth(config);
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
        System.out.println("Acquired handle for service " + service +
                           " at endpoint " + endpoint);
        return handle;
    }

    /*
     * This method contains all service-specific code in this program
     */
    private void configureAuth(NoSQLHandleConfig config) {
        if (service.equals("cloud")) {
            try {
                /* By default, look for credentials in $HOME/.oci/config */
                SignatureProvider authProvider = new SignatureProvider();

                /*
                 * Credentials can be provided directly by editing the
                 * appropriate information into the parameters below
                   authProvider = new SignatureProvider(tenantId,       // OCID
                                                        userId,         // OCID
                                                        fingerprint, // String
                                                        privateKeyFile, // File
                                                        passphrase);  // char[]
                */
                config.setAuthorizationProvider(authProvider);
            } catch (IOException ioe) {
                System.err.println("Unable to configure authentication: " +
                                   ioe);
                System.exit(1);
            }
        } else if (service.equals("onprem")) {
            config.setAuthorizationProvider(new StoreAccessTokenProvider());
        } else if (service.equals("cloudsim")) {
            /* cloud simulator */
            config.setAuthorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorizationString(Request request) {
                        return "Bearer cloudsim";
                    }

                    @Override
                    public void close() {
                    }
                });
        } else {
            System.err.println("Unknown service: " + service);
            usage();
        }
    }

    public static void main(String[] args) {

        final String tableName = "JavaQuickstart";

        /*
         * The Quickstart instance configures and acquires a handle
         */
        Quickstart qs = new Quickstart(args);

        /*
         * Configure and get a NoSQLHandle. All service specific configuration
         * is handled here
         */
        NoSQLHandle handle = qs.getHandle();
        try {

            /*
             * Create a simple table with an integer key, string name and
             * JSON data
             */
            final String createTableStatement =
                "create table if not exists " + tableName +
                "(id integer, name string, data json, primary key(id))";

            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement)
                .setTableLimits(new TableLimits(10, 10, 10));
            /* this call will succeed or throw an exception */
            handle.doTableRequest(tableRequest,
                                  20000, /* wait up to 20 sec */
                                  1000); /* poll once per second */

            System.out.println("Created table " + tableName + " ...");

            /*
             * Construct a row to put
             */
            MapValue value = new MapValue()
                .put("id", 123)
                .put("name", "joe")
                .putFromJson("data", "{\"a\": 1, \"b\": 2}", null);

            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);

            PutResult putRes = handle.put(putRequest);

            System.out.println("Put row, result " + putRes);

            /*
             * Get a row using the primary key
             */
            MapValue key = new MapValue().put("id", 123);
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            GetResult getRes = handle.get(getRequest);

            System.out.println("Got row, result " + getRes);

            /*
             * Drop the table
             */
            tableRequest = new TableRequest()
                .setStatement("drop table if exists " + tableName);

            handle.doTableRequest(tableRequest,
                                  20000,
                                  1000);
            System.out.println("Dropped table " + tableName + ", done...");
        } finally {
            /* Shutdown handle so the process can exit. */
            handle.close();
        }
    }
}

```

## Examples

Several example programs are provided in the examples directory to
illustrate the API. They can be found in the release download from GitHub or
directly in [GitHub NoSQL Examples](https://github.com/oracle/nosql-java-sdk/tree/master/examples). These examples can be run against the Oracle NoSQL
Database, the NoSQL Database Cloud Service or an instance of the Oracle
NoSQL Cloud Simulator. The code that differentiates among the configurations
is in the file Common.java and can be examined to understand the differences.

### Running Examples from a Repository Clone

Examples can be run directly from a clone of the [GitHub Repository](https://oracle.github.io/nosql-java-sdk/). Once the clone has been built (mvn compile) examples can
be run in this manner

Run BasicTableExample using a cloud simulator instance on endpoint
localhost:8080

```
$ mvn -pl examples exec:java -Dexec.mainClass=BasicTableExample \
  -Dexec.args="http://localhost:8080"
```

Run BasicTableExample using an on-premise  instance on endpoint
localhost:8090

```
$ mvn -pl examples exec:java -Dexec.mainClass=BasicTableExample \
  -Dexec.args="http://localhost:8090 -useKVProxy"
```

Run BasicTableExample using the cloud service on region us-ashburn-1

```
$ mvn -pl examples exec:java -Dexec.mainClass=BasicTableExample \
  -Dexec.args="us-ashburn-1"
```

### Compile and Run Examples using a Downloaded Release

Compile Examples:

    $ cd examples
    $ javac -cp ../lib/nosqldriver.jar *.java


#### Run using the Oracle NoSQL Database Cloud Service

This requires Oracle Cloud credentials.  Credentials can be provided directly in
API or in a configuration file. The default configuration in
examples/Common.java uses a configuration file in ~/.oci/config with the
following contents:

    [DEFAULT]
    tenancy=<User OCID>
    user=<Tenancy OCID>
    fingerprint=<Public key fingerprint>
    key_file=<PEM private key file>
    pass_phrase=<Private key passphrase>

Run the example using an Oracle Cloud region endpoint.

    $ java -cp .:../lib/nosqldriver.jar BasicTableExample <region>
        e.g.
    $ java -cp .:../lib/nosqldriver.jar BasicTableExample us-ashburn-1

The region argument will change depending on which region you use.

#### Run using the Oracle NoSQL Database On-premise

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

#### Run using the Oracle NoSQL Database Cloud Simulator

Run against the Oracle NoSQL Cloud Simulator using its default endpoint
of localhost:8080, assuming that the Cloud Simulator has been started. If
started on a different host or port adjust the endpoint accordingly.

    $ java -cp .:../lib/nosqldriver.jar BasicTableExample localhost:8080

## Licenses

See the [LICENSE](LICENSE.txt) file.

The [THIRD\_PARTY\_LICENSES](THIRD_PARTY_LICENSES.txt) file contains third
party notices and licenses.

## Help

* Open an issue in the [Issues](https://github.com/oracle/nosql-java-sdk/issues) page
* Post your question on the [Oracle NoSQL Database Community](https://community.oracle.com/community/groundbreakers/database/nosql_database).
* [Email to nosql\_sdk\_help\_grp@oracle.com](mailto:nosql_sdk_help_grp@oracle.com)

When requesting help please be sure to include as much detail as possible,
including version of the SDK and **simple**, standalone example code as needed.

## Contributing
See [CONTRIBUTING](./CONTRIBUTING.md) for details.

## Security
See [SECURITY](./SECURITY.md) for details.
