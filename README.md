# Nessie Iceberg Table Generator

It's a quick and dirty utility to generate test Iceberg tables on the Nessie catalog. The build creates a fat jar in the target directory for easy use.

Once the jar is launched, choose the supported filesystem.
```
java -jar target/nessie-iceberg-content-generator-1.0-SNAPSHOT.jar 
Missing required subcommand
Usage: iceberg-catalog-migrator [-hV] [COMMAND]
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  s3     Generate the tables using S3 as the filesystem store for Iceberg metadata and data files
  local  Generate the tables using the local path as warehouse for Iceberg metadata and data files
```

#### Example run using S3 based warehouse location
```
java -jar nessie-iceberg-content-generator-1.0-SNAPSHOT.jar s3 --nessie-uri=http://localhost:19120/api/v2 --warehouse=s3://harshm-test/nessie --tables-count=10 --snapshots-count=2 --aws-access-key=AKIAIOSFODNN7EXAMPLE  --aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
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

#### Example run using local warehouse location (without optionals)
```
java -jar target/nessie-iceberg-content-generator-1.0-SNAPSHOT.jar local --warehouse=/tmp/warehouse
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

> **INFO**: It's possible to achieve these use-cases by tweaking the logic
