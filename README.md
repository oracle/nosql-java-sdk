# Documentation for the Oracle NoSQL Database SDK for Java

This is a README for the gh-pages branch of the
[Oracle NoSQL Database SDK for Java repository](https://github.com/oracle/nosql-java-sdk). This branch is used to publish documentation on GitHub via GitHub pages

## Building and Publishing Documentation

Generated documentation is published on
[GitHub Pages](https://oracle.github.io/nosql-java-sdk/) using the GitHub Pages
facility. Publication is automatic based on changes pushed to this (gh-pages)
branch of the
[Oracle NoSQL Database SDK for Java](https://github.com/oracle/nosql-java-sdk)
repository.

In these instructions <nosql-java-sdk> is the path to a current clone from
which to publish the documentation and <nosql-java-sdk-doc> is the path to
a fresh clone of the gh-pages branch (see instructions below).

Clone the gh-pages branch of the SDK repository

``` bash
$ git clone --single-branch --branch gh-pages https://github.com/oracle/nosql-java-sdk.git nosql-java-sdk-doc
```

Generate documentation in the master (or other designated) branch of the
repository

``` bash
$ cd <nosql-node-sdk>
$ mvn clean javadoc:javadoc
```

The doc ends up in driver/target/apidocs, copy it to the gh-pages branch

``` bash
$ cp -r <nosql-java-sdk>/driver/target/apidocs/* <nosql-java-sdk-doc>
```

Commit and push after double-checking the diff in the nosql-java-sdk-doc
repository

``` bash
 $ cd <nosql-java-sdk-doc>
 $ git add .
 $ git commit
 $ git push
```

The new documentation will automatically be published to
[GitHub Pages](https://oracle.github.io/nosql-java-sdk).
