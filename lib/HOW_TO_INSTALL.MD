### Glue support

In order to support federation to AWS Glue catalog these two jars are added as libraries which have to be installed manually like so:
```
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive-client-1.10.0-WD.pom -DpomFile=lib/aws-glue-datacatalog-hive-client-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/shims-1.10.0-WD.pom -DpomFile=lib/shims-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/shims-common-1.10.0-WD.jar -DpomFile=lib/shims-common-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/hive2-shims-1.10.0-WD.jar -DpomFile=lib/hive2-shims-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/spark-hive-shims-1.10.0-WD.jar -DpomFile=lib/spark-hive-shims-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/shims-loader-1.10.0-WD.jar -DpomFile=lib/shims-loader-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-client-common-1.10.0-WD.jar -DpomFile=lib/aws-glue-datacatalog-client-common-1.10.0-WD.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive2-client-1.10.0-WD.jar -DpomFile=lib/aws-glue-datacatalog-hive2-client-1.10.0-WD.pom
```

The source of these can be found here: https://github.com/ExpediaGroup/aws-glue-data-catalog-client-for-apache-hive-metastore
