### Glue support

In order to support federation to AWS Glue catalog these two jars are added as libraries which have to be installed manually like so:
```
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive-client-3.4.0-WD-1-SNAPSHOT.pom -DpomFile=lib/aws-glue-datacatalog-hive-client-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/shims-3.4.0-WD-1-SNAPSHOT.pom -DpomFile=lib/shims-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/shims-common-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/shims-common-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/hive3-shims-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/hive3-shims-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/spark-hive-shims-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/spark-hive-shims-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/shims-loader-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/shims-loader-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-client-common-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/aws-glue-datacatalog-client-common-3.4.0-WD-1-SNAPSHOT.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive3-client-3.4.0-WD-1-SNAPSHOT.jar -DpomFile=lib/aws-glue-datacatalog-hive3-client-3.4.0-WD-1-SNAPSHOT.pom
```

The source of these can be found here: https://github.com/ExpediaGroup/aws-glue-data-catalog-client-for-apache-hive-metastore
