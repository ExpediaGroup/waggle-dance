#! /bin/sh

mvn install:install-file -Dfile=lib/shims-3.4.0-WD-1.pom -DpomFile=lib/shims-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive-client-3.4.0-WD-1.pom -DpomFile=lib/aws-glue-datacatalog-hive-client-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/shims-common-3.4.0-WD-1.jar -DpomFile=lib/shims-common-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/hive3-shims-3.4.0-WD-1.jar -DpomFile=lib/hive3-shims-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/spark-hive-shims-3.4.0-WD-1.jar -DpomFile=lib/spark-hive-shims-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/shims-loader-3.4.0-WD-1.jar -DpomFile=lib/shims-loader-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-client-common-3.4.0-WD-1.jar -DpomFile=lib/aws-glue-datacatalog-client-common-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-hive3-client-3.4.0-WD-1.jar -DpomFile=lib/aws-glue-datacatalog-hive3-client-3.4.0-WD-1.pom
mvn install:install-file -Dfile=lib/aws-glue-datacatalog-spark-client-3.4.0-WD-1.jar -DpomFile=lib/aws-glue-datacatalog-spark-client-3.4.0-WD-1.pom

