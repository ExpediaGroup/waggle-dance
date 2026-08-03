# JDK 8 and sdkman

This project uses **JDK 8**. Java is managed with sdkman (`.sdkmanrc` specifies the version).

## When running tests or Maven

- Prefer running from the project root so workspace settings apply.
- In a fresh shell, run `sdk env install` in the project root first, then `mvn test` or `mvn clean install`.
- Always use JDK 8 — do not assume a different version (e.g. system default). This
  applies both to building this repo and to rebuilding the vendored Glue client jars in
  `lib/` from the separate `aws-glue-data-catalog-client-for-apache-hive-metastore` fork
  repo — that fork's poms also target `1.8`, and CI (`main.yml`/`release.yml`) pins
  `java-version: '8'`.
