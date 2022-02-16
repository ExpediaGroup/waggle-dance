### Glue support

In order to support federation to AWS Glue catalog these two jars are added as libraries which have to be installed manually like so:
```
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-client-common-1.10.0-WD.jar -DpomFile=lib/aws-glue-datacatalog-client-common-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive2-client-1.10.0-WD.jar -DpomFile=lib/aws-glue-datacatalog-hive2-client-1.10.0-WD.pom
```

The source of these can be found here: https://github.com/patduin/aws-glue-data-catalog-client-for-apache-hive-metastore
