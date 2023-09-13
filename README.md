# Nessie Iceberg Dev Tools

## Nessie Iceberg Table Generator

It's a quick and dirty utility to generate test Iceberg tables on the Nessie catalog. The build creates a fat jar in the target directory for easy use.

Once the jar is launched, choose the supported filesystem.
```
java -jar target/nessie-dev-tools-1.0-SNAPSHOT-jar-with-dependencies.jar 
Missing required subcommand
Usage: nessie-dev-tools [-hV] [COMMAND]
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  generate-tables  Generates test tables on the catalog
  check-accessibility  Runs through the catalog and checks if the tables are accessible. Identifies the absent or denied access metadata.```

#### Example table generation using S3 based warehouse location
```
```
java -jar target/nessie-dev-tools-1.0-SNAPSHOT-jar-with-dependencies.jar generate-tables s3 --nessie-uri=http://localhost:19120/api/v2 --warehouse=s3://harshm-test/nessie --tables-count=10 --snapshots-count=2 --aws-access-key=AKIAIOSFODNN7EXAMPLE  --aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

```
Generated gentool.table153@main
Generated gentool.table156@main
Generated gentool.table155@main
Generated gentool.table157@main
<< Trimmed >>
Remaining: 0
Time taken: 13 seconds
```

#### Example table generation using local warehouse location (without optionals)
```
java -jar target/nessie-dev-tools-1.0-SNAPSHOT-jar-with-dependencies.jar generate-tables local --warehouse=/tmp/warehouse
```
```
Generated gentool.table8134@main
Generated gentool.table8196@main
Generated gentool.table8128@main
Generated gentool.table8138@main
Generated gentool.table8171@main
Generated gentool.table8140@main
Remaining: 90
Generated gentool.table8141@main
<< Trimmed >>
Generated gentool.table8124@main
Remaining: 0
Time taken: 14 seconds
```

## Unsupported Scenarios
The tool was natively built on a very basic test generation use-case. Hence, there are many combinations of FileSystem
and Nessie configurations pending. These cases can be added as needed. High level cases include -

* Non-default configurations in Nessie - example authentication conf.
* Many remote IOs - Azure, GCS etc.
* Evolved schemas, partitions, sort order, delete files, table properties.
* Different number of files per snapshot.


## Table Accessbility Checker

##### Example table accessibility inspection using S3
```
java -jar target/nessie-dev-tools-1.0-SNAPSHOT-jar-with-dependencies.jar check-accessibility s3 --aws-access-key=AKIAIOSFODNN7EXAMPLE --aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --check-heads-only=true --errors-only=true
```
```
Table [accesspath1.b1accesspath1 @ BRANCH 'b1'], Metadata [s3://bucketpath-b1/accesspath1/b1accesspath1/metadata/00001-9822836d-e92a-484f-b03f-02b67bf97223.metadata.json], Accessibility Status [ACCESS_DENIED]
Table [accesspath2.b1accesspath2 @ BRANCH 'b1'], Metadata [s3://bucketpath-b1/accesspath2/b1accesspath2/metadata/00005-0352015c-8673-4173-be59-c4f8847ec785.metadata.json], Accessibility Status [ACCESS_DENIED]
Table [b2 @ BRANCH 'b1'], Metadata [s3://bucketpath-b2/b2/metadata/00003-9df7ffba-4757-4758-aa5f-3103675135e1.metadata.json], Accessibility Status [ACCESS_DENIED]
Table [user1t2 @ BRANCH 'b1'], Metadata [s3://eng.data.com/n1/bulk/user1t2/metadata/00003-9b7e71e0-38ea-435f-9e97-3a6bf1c39ede.metadata.json], Accessibility Status [NOT_FOUND]
Table [accesspath1.b1accesspath1 @ BRANCH 'main'], Metadata [s3://bucketpath-b1/accesspath1/b1accesspath1/metadata/00001-9822836d-e92a-484f-b03f-02b67bf97223.metadata.json], Accessibility Status [ACCESS_DENIED]
Table [accesspath2.b1accesspath2 @ BRANCH 'main'], Metadata [s3://bucketpath-b1/accesspath2/b1accesspath2/metadata/00005-0352015c-8673-4173-be59-c4f8847ec785.metadata.json], Accessibility Status [ACCESS_DENIED]
Table [b2 @ BRANCH 'main'], Metadata [s3://bucketpath-b2/b2/metadata/00003-9df7ffba-4757-4758-aa5f-3103675135e1.metadata.json], Accessibility Status [ACCESS_DENIED]
```
