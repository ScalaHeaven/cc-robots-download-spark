# AGENTS.md

Guidance for coding agents working in this repository.

## Repository Purpose

This repository is a ready-to-open Scala 3 and Apache Spark development
workspace for VS Code Dev Containers. It provides:

- a Scala 3 sbt project with Common Crawl robots.txt and sitemap-link Spark
  pipelines that default to a local standalone cluster with one master and 10
  workers
- a devcontainer image with JDK 21, Node.js, Codex, Coursier, Scala CLI, sbt,
  Metals, Metals MCP, and JDK sources for Java standard library navigation
- VS Code settings for Scala editing, formatting, and debugging
- a production Dockerfile that builds a runnable assembly JAR

Treat this as both a Scala project template and a dev environment definition.
Changes often affect onboarding, editor behavior, or reproducible builds, not
just application code.

## Important Files

- `build.sbt`: main sbt build definition. Pins Scala `3.8.3`, enables
  SemanticDB, adds Spark SQL `3.5.1` through the explicit `_2.13` Spark
  artifact, adds sttp client `3.5.2`, adds jwarc `0.36.0`, adds munit `1.2.0`
  for tests, sets project metadata, configures the `sbt run` JVM heap,
  configures Spark Java module options, and configures `sbt-assembly`.
- `project/build.properties`: pins sbt `1.12.11`.
- `project/plugins.sbt`: declares sbt plugins, currently `sbt-assembly` and
  `sbt-scalafmt`.
- `src/main/scala/Main.scala`: Scala 3 Spark application entry point with the
  `Main` main class.
- `src/main/scala/CommonCrawlRobotsArchiveSupport.scala`: shared Common Crawl
  manifest download, archive download, polite HTTP delay, retry/backoff,
  download timeout, target filtering, and HTTP body decoding helpers used by
  robots.txt archive pipelines.
- `src/main/scala/CommonCrawlRobotsPipeline.scala`: parallelizes listed
  `robotstxt/*.warc.gz` archive streaming downloads with Spark, parses WARC
  responses with jwarc, validates robots.txt payloads, and saves valid
  robots.txt payload files into the configured output folder.
- `src/main/scala/CommonCrawlSitemapsPipeline.scala`: parallelizes listed
  `robotstxt/*.warc.gz` archive streaming downloads with Spark, parses valid
  robots.txt payloads, and writes extracted `Sitemap:` links to per-archive TSV
  files.
- `src/main/scala/CommonCrawlCountrySitemapsPipeline.scala`: parallelizes listed
  `robotstxt/*.warc.gz` archive streaming downloads with Spark, parses valid
  robots.txt payloads, filters extracted `Sitemap:` links to one selected
  country, and writes only matching links to per-archive TSV files.
- `src/main/scala/LocalRobotsSitemapsPipeline.scala`: recursively streams
  locally saved robots.txt files in bounded batches, parses valid payloads with
  Spark, and writes extracted `Sitemap:` links to per-batch partition TSV files.
  The `localSitemaps.batchSize` JVM property controls the default 10,000-file
  batch size.
- `src/main/scala/LocalSitemapsFilterPipeline.scala`: recursively reads local
  sitemap TSV files produced by `LocalRobotsSitemapsPipeline`, filters rows by
  the vendored country suffix database, and writes grouped country,
  language-region, or selected-country TSV files.
- `src/main/scala/LocalSitemapDownloadPipeline.scala`: recursively reads local,
  filtered, and WARC-backed country `*.sitemaps.tsv` files, treats the fourth
  TSV field as the seed sitemap URL, downloads sitemap XML files, follows
  sitemap indexes, and writes extracted page URLs to per-partition TSV files.
- `src/main/scala/SitemapCountryLocaleClassifier.scala`: pure country suffix
  and URL marker classifier used by the local sitemap filter and unit tests.
- `src/main/resources/sitemap-filter/country-suffixes.tsv`: vendored country
  suffix database for deterministic offline country matching. Keep this
  deterministic and offline unless the runtime model is deliberately changed.
- `src/main/scala/Cli.scala`: argument parsing and defaults. Accepts a
  `robotstxt.paths.gz` URL directly and resolves a sibling `wat.paths.gz` URL to
  `robotstxt.paths.gz`. Supports the default robots pipeline and the `sitemaps`
  `local-sitemaps`, `filter-sitemaps`, `country-sitemaps`,
  `local-country-sitemaps`, and `download-sitemaps` subcommands.
  Supports `--max-files N` for WARC-backed robots, sitemaps, and
  country-sitemaps runs to cap manifest archive files before Spark downloads
  begin. Supports
  `--download-connect-timeout-seconds N`,
  `--download-read-timeout-seconds N`, and
  `--download-delay-seconds N` for controlled HTTP downloads.
  `local-sitemaps`, `filter-sitemaps`, `local-country-sitemaps`, and
  `download-sitemaps` default to `local[*]`; WARC-backed `country-sitemaps`
  defaults to the standalone local cluster.
- `src/main/scala/SparkSessionFactory.scala`: configures
  `local-cluster[1,1,200]`, executor memory, Java module options, and the
  driver's active Scala library on executor classpaths.
- `.scalafmt.conf`: Scala formatting config, using Scalafmt `3.10.7` and the
  Scala 3 dialect.
- `.devcontainer/devcontainer.json`: VS Code Dev Containers entry point.
- `.devcontainer/Dockerfile`: development image definition, including JDK
  source archives linked into `JAVA_HOME` for Metals Java navigation and a
  minimal `/opt/spark` home for local cluster executors.
- `.devcontainer/post-start.sh`: repairs workspace permissions, syncs
  `/opt/spark` jars to the pinned Spark version, syncs selected host SSH/Codex
  files, configures Git, configures Codex Metals MCP, and starts Metals MCP.
- `.vscode/launch.json`: Metals/Scala debug launch config for `Main`.
- `.vscode/settings.json`: editor settings, including watcher excludes.
- `.github/workflows/release-jar.yml`: builds the assembly JAR on pushed tags
  and uploads it to the tag's GitHub Release assets.
- `Dockerfile`: production multi-stage build that runs `sbt assembly`, creates
  a minimal `/opt/spark` home, and launches `target/scala-3.8.3/app.jar`.
- `README.md`: user-facing explanation of the environment and workflows.

## Build And Validation Commands

Run sbt commands serially. Starting multiple sbt processes at the same time can
hit the sbt boot socket lock and fail with `ServerAlreadyBootingException`.

Use these commands from the repository root:

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true test
sbt -Dsbt.batch=true "run --help"
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-robots"
sbt -Dsbt.batch=true "run robots https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-robots --max-files 5000"
sbt -Dsbt.batch=true "run sitemaps https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-sitemaps"
sbt -Dsbt.batch=true "run local-sitemaps target/commoncrawl-robots target/commoncrawl-sitemaps"
sbt -Dsbt.batch=true "run filter-sitemaps target/commoncrawl-sitemaps target/filtered-sitemaps"
sbt -Dsbt.batch=true "run country-sitemaps ukraine https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/filtered-sitemaps/country/ukraine --max-files 5000"
sbt -Dsbt.batch=true "run local-country-sitemaps ukraine target/commoncrawl-sitemaps target/filtered-sitemaps/country/ukraine"
sbt -Dsbt.batch=true "run download-sitemaps target/filtered-sitemaps/country/ukraine target/downloaded-sitemap-links local[1]"
sbt -Dsbt.batch=true assembly
docker build -t cc-robots-download-spark .
docker run --rm cc-robots-download-spark
```

For formatting, prefer the configured Scala formatter:

```bash
sbt -Dsbt.batch=true scalafmtCheckAll
sbt -Dsbt.batch=true scalafmtAll
```

## Coding Guidelines

- Follow idiomatic Scala 3 syntax and keep Spark transformations readable.
- Preserve the `Main` entry point or update every dependent reference
  when renaming it, including `build.sbt`, `.vscode/launch.json`, `README.md`,
  and Docker-related documentation.
- Keep `build.sbt` small and declarative. Add dependencies or plugins only when
  they support the requested behavior. Keep Spark dependencies explicit.
- Keep devcontainer scripts idempotent. They are run on creation and on start,
  so repeated execution must be safe.
- Keep devcontainer workspace paths aligned with
  `/workspaces/cc-robots-download-spark`; Metals and Metals MCP write local
  state under that workspace path.
- Preserve non-root `vscode` user behavior in container changes unless the task
  explicitly requires a different model.
- Prefer Coursier-based tool installation patterns already used in the
  devcontainer and production Dockerfile.
- Keep Docker layer ordering cache-friendly: copy build metadata before source
  files when dependency resolution can be cached.
- Keep Common Crawl manifest handling aligned with the archive paths consumed by
  `CommonCrawlRobotsPipeline` and `CommonCrawlSitemapsPipeline`.
- Keep sitemap country filtering deterministic and offline. Update the vendored
  suffix database and tests together if target country suffixes change.
- Use sttp for HTTP downloads and keep downloads streaming to files rather than
  buffering archive contents in memory. Preserve the one-second default HTTP
  request delay, Common Crawl 403 rate-limit retries, and retry/backoff behavior
  for transient download failures.
- Use jwarc for WARC parsing instead of ad hoc string parsing.
- Update `README.md` whenever commands, tool versions, archive handling, startup
  behavior, or the mental model for users changes.

## Scala 3 Code Style

Prefer clear, typed, idiomatic Scala 3 over cleverness. Code in this repository
should be easy for newcomers to read and easy for tools to analyze.

### Structure And Naming

- Use descriptive names for values, functions, types, and files. Avoid
  abbreviations unless they are standard in the domain.
- Use `UpperCamelCase` for classes, traits, objects, and type aliases.
- Use `lowerCamelCase` for methods, values, variables, parameters, and packages.
- Keep one primary public type per file when the project grows beyond examples.
  Match the file name to that type.
- Prefer small functions with one clear responsibility. Split logic when a
  function needs unrelated comments to explain its phases.
- Put domain logic in named functions, classes, traits, or objects. Keep `main`
  methods thin and focused on wiring, parsing input, and reporting output.

### Types And APIs

- Let local values infer obvious types, but write explicit return types on public
  methods, non-trivial private methods, recursive methods, and extension
  methods.
- Prefer immutable `val` values. Use `var` only for tightly scoped mutation where
  it is simpler and still easy to reason about.
- Prefer algebraic data types with `sealed trait`, `case class`, and case
  objects over loosely typed strings, maps, or flags.
- Use `Option` for optional values instead of `null`.
- Use `Either`, `Try`, or a small domain error type for recoverable failures
  instead of throwing exceptions across normal control flow.
- Throw exceptions only for programmer errors, impossible states, or integration
  boundaries where an exception is the expected API.
- Avoid `Any`, `asInstanceOf`, reflection, and unchecked pattern matches unless
  there is a strong reason and the code explains the boundary.
- Avoid public APIs that expose mutable collections. Prefer immutable collections
  from the Scala standard library.

### Scala 3 Language Features

- Use braces and parentheses consistently, as already shown in
  `src/main/scala/Main.scala`.
- Keep `-Yexplicit-nulls` compatibility in mind. Use explicit `T | Null` types
  at Spark integration boundaries where external data or Spark SQL may produce
  nulls.
- Prefer `sealed trait` plus `case object` or `case class` for closed sets of
  alternatives. Add methods on the sealed hierarchy when behavior naturally
  belongs with the alternatives.
- Prefer pattern matching for algebraic data types, but keep matches exhaustive
  and avoid large nested matches. Extract helper functions when branches grow.
- Do not introduce advanced type-level programming unless the requested feature
  genuinely benefits from it and the resulting API remains understandable.

### Collections And Control Flow

- Prefer collection transformations such as `map`, `flatMap`, `filter`,
  `collect`, `foldLeft`, and `exists` when they read directly.
- Prefer a straightforward `for` expression for multi-step `Option`, `Either`,
  `Try`, `Future`, or collection workflows.
- Avoid using `map` only for side effects. Use `foreach` for side effects.
- Avoid deeply chained transformations when intermediate names would make the
  code clearer.
- Be mindful of partial methods such as `head`, `tail`, `last`, and `.get` on
  `Option`. Prefer safe alternatives such as pattern matching, `headOption`, or
  `fold`.

### Effects, IO, And Concurrency

- Keep side effects at the edges of the program. Pure functions should take
  inputs and return values without printing, reading files, mutating global
  state, or depending on the current time.
- If adding concurrency or asynchronous behavior, choose a clear model and keep
  it consistent. Do not mix `Future`, threads, blocking calls, and callbacks
  casually.
- Mark blocking operations clearly and isolate them from pure domain logic.
- Avoid global mutable state. If shared state is required, make ownership and
  synchronization explicit.

### Error Messages And User Output

- Make user-facing messages specific and actionable.
- Include enough context in failures to diagnose the problem, but do not print
  secrets, tokens, private keys, or local machine-specific credentials.
- Keep example application output stable unless the task is specifically about
  changing behavior.

### Dependencies

- Prefer the Scala standard library for small features.
- Add dependencies only when they reduce real complexity or provide a proven
  implementation for a non-trivial concern.
- Keep dependency versions explicit in `build.sbt` or a deliberate version
  management file if one is introduced later.
- When adding an sbt plugin, document what it is for and update validation
  commands if it adds or changes tasks.

### Testing Expectations

- This repository uses munit for focused Scala tests. If adding meaningful
  business logic or behavior with edge cases, add tests instead of relying only
  on `sbt run`.
- Prefer focused unit tests around pure functions and domain behavior.
- Test error cases, boundary inputs, and at least one representative successful
  path.
- Keep tests deterministic. Avoid depending on wall-clock time, host-specific
  paths, network access, or command execution order unless that is the behavior
  under test.

### Comments And Documentation

- Write comments to explain why a non-obvious decision exists, not to restate
  what each line does.
- Keep Scaladoc concise and useful for public APIs once the project exposes
  reusable types or functions.
- When changing environment behavior, update `README.md` and this file so agents
  and humans have the same source of truth.

## Quality Bar For Agent Changes

Before finishing a code change, agents should check:

- The code compiles with `sbt -Dsbt.batch=true compile`.
- Formatting is clean with `sbt -Dsbt.batch=true scalafmtCheckAll`.
- New behavior is covered by tests when the change adds non-trivial logic.
- Public entry point names still match `build.sbt`, `.vscode/launch.json`,
  `README.md`, and Docker documentation.
- No generated build output or local tool state is included in the change.
- The implementation follows existing repository patterns before introducing new
  abstractions, dependencies, or tooling.

## Generated And Local Files

Do not hand-edit generated or local state unless specifically investigating tool
behavior:

- `.bsp/`
- `.metals/`
- `target/`
- `project/target/`
- `.scala-build/`
- `.bloop/`

These paths may be present in a working tree because Metals, sbt, or Scala CLI
created them. Avoid committing generated build output.

## Current Capabilities

The repository currently supports:

- compiling and running a Scala 3 Spark app with sbt
- downloading Common Crawl `robotstxt.paths.gz` manifests
- streaming listed robotstxt WARC archives in parallel with Spark and sttp
- controlling HTTP download connect and read timeouts for sitemap downloads
- applying a default one-second HTTP request delay across download commands
- extracting robots.txt response payloads with jwarc
- extracting `Sitemap:` links from valid parsed robots.txt payloads
- extracting `Sitemap:` links from locally saved robots.txt payload files with
  bounded driver memory
- starting one local Spark master and 1 local Spark workers by default
- writing extracted robots.txt files grouped by target host
- writing extracted sitemap links as per-archive TSV files
- writing selected-country sitemap links from robotstxt WARC archives without
  saving intermediate robots.txt files
- writing locally extracted sitemap links as per-partition TSV files
- filtering local sitemap TSV rows by vendored country suffixes with grouped
  country, language-region, and selected-country output
- downloading sitemap XML from local, filtered, and WARC-backed country sitemap
  TSV rows and writing extracted page URLs
- producing a fat JAR with `sbt-assembly`
- publishing the assembly JAR as a GitHub Release asset when a tag is pushed
- building and running a production Docker image
- editing and debugging Scala through VS Code Metals
- navigating into Java standard library sources from Metals
- exposing Metals MCP to Codex from inside the devcontainer
- syncing selected host SSH and Codex configuration into the container
- repairing common ownership and permission issues after container startup

If expanding pipeline behavior with meaningful branching logic, add focused
munit coverage and document any new validation command here and in `README.md`.

## Verification Notes

At the time this file was added, these commands passed:

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true test
sbt -Dsbt.batch=true assembly
sbt -Dsbt.batch=true scalafmtCheckAll
```

The workspace uses JDK 21 for Spark compatibility.
