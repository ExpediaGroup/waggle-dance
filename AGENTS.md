# Waggle Dance

Waggle Dance is a Hive Metastore Thrift proxy that federates requests across multiple
Hive Metastore (and AWS Glue Data Catalog) deployments behind a single virtual endpoint.
Clients (Hive, Spark, Trino, etc.) connect to one Waggle Dance Thrift URI; Waggle Dance
resolves the virtual database name to a mapped metastore and forwards the call.

## Goals
- Correctness of federated Thrift calls — a bug here breaks metadata access for every
  client of the federated database, not just one caller.
- Backward compatibility for existing virtual database mappings — config changes must
  not silently redirect or break existing client traffic.
- Support both native Hive Metastore and AWS Glue Data Catalog as federated backends.

## Constraints
- Must remain compatible with Hive 3.x Thrift API clients (Spark, Trino, Athena-style
  consumers included).
- The bundled AWS Glue Data Catalog client (see "Vendored Glue client jars" below) pins
  the toolchain to **JDK 8** — this is not aspirational, it's load-bearing.
- This is an open-source project (Apache-2.0, published to Maven Central via
  `.github/workflows/release.yml`) — treat `CONTRIBUTING.md`/`CODE-OF-CONDUCT.md` as
  binding for any external-facing process changes.

## Project structure

Multi-module Maven project (parent `pom.xml`, `com.expediagroup:waggle-dance-parent`):

```
waggle-dance-api/              # Interfaces / contracts shared across modules
waggle-dance-core/             # Core federation logic (Thrift handler, metastore mapping)
waggle-dance-rest/             # REST admin endpoints
waggle-dance-boot/             # Spring Boot executable assembly
waggle-dance-extensions/       # Optional extensions (e.g. rate limiting)
waggle-dance-integration-tests/
waggle-dance/                  # TGZ distribution assembly
waggle-dance-rpm/              # RPM distribution assembly
lib/                           # Vendored third-party jars (see below) — NOT built by this repo
```

## Vendored Glue client jars (`lib/`)

`lib/*.jar` and `lib/*.pom` are **pre-built artifacts from a separate repo**:
[`aws-glue-data-catalog-client-for-apache-hive-metastore`](https://github.com/ExpediaGroup/aws-glue-data-catalog-client-for-apache-hive-metastore)
(EG fork, `branch/waggle-dance`), version `3.4.0-WD-1`. They are installed into the local
Maven repo with `lib/install_local_libs.sh` before this project can build — this is a
manual step, not automated by `mvn install` here. See `lib/HOW_TO_INSTALL.MD`.

**Never** bump the jar filename/version to pick up a fork change without actually
rebuilding and swapping the jar — the version string in `pom.xml`
(`waggle-dance-core/pom.xml`, `com.amazonaws.glue:aws-glue-datacatalog-hive3-client`) and
the jar's *actual* bundled bytecode must stay in sync; there's no CI step that verifies
this for you.

**Always build the fork with JDK 8**, matching the fork's `pom.xml` `<source>1.8</source>`
and this repo's own CI (`.github/workflows/main.yml` / `release.yml` pin
`java-version: '8'`). Building the fork with a different JDK (e.g. whatever a local
SDKMAN default happens to be) still produces a working jar in most cases, but the
manifest's `Build-Jdk-Spec` will not match — verify it explicitly:
```bash
unzip -p lib/aws-glue-datacatalog-hive3-client-3.4.0-WD-1.jar META-INF/MANIFEST.MF | grep Build-Jdk-Spec
# expect: Build-Jdk-Spec: 1.8
```
When rebuilding, verify the fix is actually bundled by disassembling the changed class
(`javap -p -c <ClassName>.class`) and diffing against the previous jar — do not trust the
build log alone; the previous jar's `Created-By`/`Build-Jdk-Spec` can silently drift from
JDK 8 if `sdk use java` isn't set explicitly in the build shell.

## CHANGELOG.md convention

This repo does **not** use a `## [Unreleased]` heading (that convention was abandoned
years ago). Every fix/feature entry is committed directly under a **resolved**
`## [version] - YYYY-MM-DD` heading, in its own commit, at the time the change is made —
matching whatever version is currently set in the root `pom.xml` (`<version>` on
`waggle-dance-parent`, which is always a `-SNAPSHOT` of the *next* release).

- Check `pom.xml`'s current `<version>` (strip `-SNAPSHOT`) before adding an entry — that
  is the version heading to use, with today's date.
- The `maven-release-plugin` commits (`prepare release ...` / `prepare for next
  development iteration ...`) never touch `CHANGELOG.md`. The changelog entry for a
  change must always be added in its own separate, manual commit — don't expect the
  release process to write it for you, and don't skip it assuming it will be generated.
- Follow the existing per-entry format: `### Fixed` / `### Added` / `### Changed` bullet
  groups, one bullet per notable change, with a GitHub link to the relevant PR/commit
  when the change traces to a specific upstream commit (see recent entries for the
  pattern).

## Build

```bash
lib/install_local_libs.sh        # one-time / after any lib/ jar change: install vendored jars
mvn clean install                # build all modules
mvn clean test                   # run tests
mvn clean package -DskipTests    # build without tests
```

## Testing Standards
- Unit tests live in `src/test/java` per module; add/update tests alongside any logic
  change, especially in `waggle-dance-core` (the federation/Thrift handler logic).
- `waggle-dance-integration-tests` covers end-to-end federation behavior — run it for any
  change touching request routing or metastore mapping resolution.
- Tests must pass before merging (`mvn clean install`, no `-DskipTests`) unless a failure
  is pre-existing and unrelated (verify by checking `git log` on the failing test file —
  if it predates your change and isn't touched by it, note this in the PR rather than
  silently skipping tests repo-wide).

## Boundary Conditions

### Always Ask Before
- Modifying virtual database mapping / federation config semantics.
- Changing the vendored Glue client jar version or replacing `lib/*.jar` contents.
- Modifying `.github/workflows/release.yml` or `main.yml` (CI/release pipeline).
- Pushing to `main`.

### Never Do
- Commit without explicit instruction.
- Skip tests to work around a real (not pre-existing/unrelated) failure.
- Change the `lib/` jar version string without also rebuilding and verifying the actual
  jar contents match (see "Vendored Glue client jars" above).
- Add a `## [Unreleased]` heading to `CHANGELOG.md` — use a resolved version + date (see
  "CHANGELOG.md convention" above).

## Documentation Standards
- Update `CHANGELOG.md` in its own commit for every notable change (see convention above).
- Update `lib/HOW_TO_INSTALL.MD` if the vendored jar set or install steps change.
- Review `README.md`/`HowToKerberize.md` for impact when changing federation, Kerberos,
  or install-facing behavior.

## Git Workflow

### Branching
Always create a new branch before starting work; never commit directly to `main`.

### Branch Naming
`{type}/<short-description>` (e.g. `fix/update-glue-datacatalog-hive3-client-jar-npe`,
`docs/agents-md`). Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.

### Commit Messages
```
type: short description

Longer explanation if needed.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
```

### AI-Assisted Commits
**Required**: every commit involving AI assistance includes a `Co-Authored-By` trailer
for the AI tool used.

### Pull Requests
- Summary + test plan in the description.
- Two human reviewers required to merge (per `CONTRIBUTING.md`).
- All CI checks (`main.yml`) must pass.
