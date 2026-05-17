# Scala 3 Spark Devcontainer

Ready-to-open Scala 3 workspace for downloading Common Crawl robots.txt
captures with Apache Spark in VS Code Dev Containers.

This repository gives you:

- Scala `3.8.3`, sbt `1.12.11`, and Apache Spark SQL `3.5.1`
- sttp client `3.5.2` for streaming HTTP downloads
- jwarc `0.36.0` for reading compressed WARC archives
- a VS Code devcontainer with JDK 21, Scala CLI, sbt, Metals, Codex, and Metals MCP
- JDK source archives linked into the devcontainer JDK for Java standard library
  navigation from Metals
- a production Dockerfile that builds and runs an assembly JAR

The app streams a Common Crawl `robotstxt.paths.gz` manifest to a temporary
file with sttp, reads the archive paths inside it, launches Spark jobs to stream
the listed `robotstxt/*.warc.gz` archives in parallel with retry/backoff,
extracts robots.txt response payloads, and writes the extracted text files to an
output folder.

## Quick Start

Open this folder in VS Code, then run **Dev Containers: Reopen in Container**.
The container opens at `/workspaces/spark-scala3-cluster-devcontainer`.

Inside the container:

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-robots"
```

The app also accepts a sibling `wat.paths.gz` URL and resolves it to
`robotstxt.paths.gz` before downloading:

```bash
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/wat.paths.gz target/commoncrawl-robots"
```

Show CLI usage:

```bash
sbt -Dsbt.batch=true "run --help"
```

## Runtime Defaults

When no arguments are supplied, the app uses:

```text
paths_gz_url https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz
output_dir   target/commoncrawl-robots
spark_master local-cluster[10,1,200]
```

`local-cluster[10,1,200]` starts one local standalone master and 10 worker
JVMs. Each worker has one core and 200 MiB of worker memory. Executors are
configured with `spark.executor.memory=200m`, and Spark's reserved-memory floor
is disabled with `spark.testing.reservedMemory=0` so the low-memory development
cluster can start. The driver JVM uses `-Xmx4g` for `sbt run`, VS Code debug
launches, and the production Docker image.

To target another Spark master, pass it as the third argument:

```bash
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz /data/commoncrawl-robots spark://spark-master:7077"
```

For external clusters, make sure the output path is reachable and writable from
executors, because each archive extraction runs on Spark workers.

## Output Layout

Extracted robots.txt payloads are written beneath the requested output
directory, grouped by target host:

```text
target/commoncrawl-robots/example.com/https-2026-04-10T08_11_53Z-a1b2c3d4e5f60789.txt
```

Filenames include the URI scheme, WARC capture date, and a short SHA-256 digest
derived from the target URI, capture date, and archive path to avoid collisions.

The output directory is not deleted before a run. Existing files with the same
generated name are replaced.

## How The Robots Pipeline Works

`CommonCrawlRobotsPipeline` is the main data pipeline behind the application.
It takes the parsed CLI configuration, downloads the Common Crawl manifest,
uses Spark to process each listed robots.txt WARC archive in parallel, and
writes one text file for each robots.txt response record it finds.

The pipeline starts by normalizing the manifest URL. A direct
`robotstxt.paths.gz` URL is used as-is. A sibling `wat.paths.gz` URL is
rewritten to `robotstxt.paths.gz`, which makes it convenient to copy either
kind of Common Crawl paths URL from the crawl index page.

The manifest is streamed to a temporary `.gz` file with sttp, then read through
`GZIPInputStream`. Empty lines and comments are ignored, and only archive paths
that contain `/robotstxt/` and end in `.warc.gz` are kept. Relative archive
paths are resolved against `https://data.commoncrawl.org/`; absolute paths are
used directly.

Spark receives the filtered archive path list with one partition per archive
path. Each worker downloads its assigned archive to a temporary `.warc.gz` file,
retrying HTTP downloads up to four times with exponential backoff. Local
`file:` URIs and plain local paths are also supported, which is useful for
small manual tests.

Each downloaded archive is parsed with jwarc in lenient mode. The pipeline
keeps only `WarcResponse` records whose target URI path ends with
`/robots.txt`, case-insensitively. Non-matching records are consumed and skipped
so the archive stream can continue without buffering record bodies in memory.

For each matching response, the HTTP body stream is decoded according to the
record's `Content-Encoding` header. `gzip`, `x-gzip`, and `deflate` encodings
are handled, including stacked encodings, and the decoded body is copied
directly to the output file.

Output files are grouped by lowercased target host. The file name includes the
URI scheme, WARC capture date, and the first 16 hex characters of a SHA-256
digest over the target URI, capture date, and source archive path. This keeps
names readable while avoiding collisions between repeated captures.

Each archive task reports the number of files it saved or the failure it hit.
After Spark collects the task results, the driver prints a run summary. If any
archive failed to download or extract, the driver prints each failed archive and
raises an exception so the overall run exits unsuccessfully.

## Build And Validation

Run sbt commands serially. Starting multiple sbt processes at the same time can
hit the sbt boot socket lock and fail with `ServerAlreadyBootingException`.

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true "run --help"
sbt -Dsbt.batch=true assembly
sbt -Dsbt.batch=true scalafmtCheckAll
```

Format Scala sources with:

```bash
sbt -Dsbt.batch=true scalafmtAll
```

Build and run the production image:

```bash
docker build -t spark-scala3-cluster-devcontainer .
docker run --rm spark-scala3-cluster-devcontainer
```

## Project Layout

- `build.sbt`: pins Scala, Spark SQL, sttp, and jwarc; enables SemanticDB;
  configures Spark Java module options; sets the `sbt run` heap; and builds
  `app.jar` with `sbt-assembly`.
- `src/main/scala/Main.scala`: application entry point.
- `src/main/scala/Cli.scala`: argument parsing and runtime defaults.
- `src/main/scala/SparkSessionFactory.scala`: Spark session construction,
  local-cluster driver binding, executor memory, Java module options, and
  Scala library classpath handling for executor RPC serialization.
- `src/main/scala/CommonCrawlRobotsPipeline.scala`: sttp streaming manifest
  download, retrying Spark parallel archive download, WARC parsing, and
  robots.txt file writes.
- `.devcontainer/Dockerfile`: development image with JDK 21, Scala tools, JDK
  sources for Metals navigation, and a minimal Spark home.
- `.devcontainer/post-start.sh`: idempotent startup repair and tool setup.
- `.vscode/launch.json`: Metals/Scala debug launch config for `Main`.
- `Dockerfile`: production multi-stage build that builds the assembly JAR and
  runs it with the minimal Spark home.

## Spark And Scala Notes

Spark `3.5.1` is consumed through the explicit `spark-sql_2.13` artifact because
Spark publishes its Scala APIs for Scala 2.13. Application sources compile with
Scala `3.8.3`, while `SPARK_SCALA_VERSION` remains `2.13` for local-cluster
executor startup.

Local-cluster executors receive the driver's active Scala library first on their
classpath so Spark RPC serialization uses a matching Scala standard library on
both sides.
