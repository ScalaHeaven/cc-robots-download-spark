# Scala 3 Spark Devcontainer

Ready-to-open Scala 3 workspace for downloading Common Crawl robots.txt
captures with Apache Spark in VS Code Dev Containers.

This repository gives you:

- Scala `3.8.3`, sbt `1.12.11`, and Apache Spark SQL `3.5.1`
- sttp client `3.5.2` for streaming HTTP downloads
- jwarc `0.36.0` for reading compressed WARC archives
- munit `1.2.0` for focused Scala tests
- a VS Code devcontainer with JDK 21, Scala CLI, sbt, Metals, Codex, and Metals MCP
- JDK source archives linked into the devcontainer JDK for Java standard library
  navigation from Metals
- a production Dockerfile that builds and runs an assembly JAR

The app streams a Common Crawl `robotstxt.paths.gz` manifest to a temporary
file with sttp, reads the archive paths inside it, launches Spark jobs to stream
the listed `robotstxt/*.warc.gz` archives in parallel with retry/backoff, and
can either save valid robots.txt payloads or extract sitemap links from parsed
robots.txt files.

## Quick Start

Open this folder in VS Code, then run **Dev Containers: Reopen in Container**.
The container opens at `/workspaces/spark-scala3-cluster-devcontainer`.

Inside the container:

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-robots"
```

To process only the first 5,000 robotstxt archive files from the manifest:

```bash
sbt -Dsbt.batch=true "run https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-robots --max-files 5000"
```

To extract sitemap links from parsed robots.txt files instead:

```bash
sbt -Dsbt.batch=true "run sitemaps https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz target/commoncrawl-sitemaps"
```

To extract sitemap links from robots.txt files that were already downloaded
locally:

```bash
sbt -Dsbt.batch=true "run local-sitemaps target/commoncrawl-robots target/commoncrawl-sitemaps"
```

When the output path is omitted, `local-sitemaps` writes to
`target/commoncrawl-sitemaps`.

To filter local sitemap TSV files down to Ukraine, Russia, and United Kingdom
domains and write country and language-region groups:

```bash
sbt -Dsbt.batch=true "run filter-sitemaps target/commoncrawl-sitemaps target/filtered-sitemaps"
```

When the output path is omitted, `filter-sitemaps` writes to
`target/filtered-sitemaps`.

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

The default command saves valid robots.txt files. Prefix arguments with
`sitemaps` to run the WARC-backed sitemap-link pipeline instead. Prefix
arguments with `local-sitemaps` to parse an existing local robots.txt output
directory. The default local robots input directory is
`target/commoncrawl-robots`, and the default sitemap output directory is
`target/commoncrawl-sitemaps`. Unlike the WARC-backed pipelines,
`local-sitemaps` defaults to `local[*]` instead of `local-cluster[10,1,200]`
to avoid starting standalone Spark master and worker JVMs for local file
parsing.

Prefix arguments with `filter-sitemaps` to read local sitemap TSV files from
`target/commoncrawl-sitemaps` by default and write filtered output to
`target/filtered-sitemaps`. This subcommand also defaults to `local[*]`.

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

To cap a WARC-backed run before Spark starts downloading archives, pass
`--max-files N`. The option applies to the default robots command and the
`sitemaps` command, and limits the number of `robotstxt/*.warc.gz` archive
files read from the manifest:

```bash
sbt -Dsbt.batch=true "run robots https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-17/robotstxt.paths.gz /data/commoncrawl-robots spark://spark-master:7077 --max-files 5000"
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
generated name are replaced, and a same-name robots.txt file is removed if a
later run rejects that capture as invalid.

The sitemap-link pipeline writes one TSV file per processed archive beneath the
requested output directory:

```text
target/commoncrawl-sitemaps/archive-a1b2c3d4e5f60789.sitemaps.tsv
```

Each row contains four tab-separated fields: source archive path, WARC capture
date, robots.txt URL, and sitemap URL. Archive-level files avoid concurrent
Spark workers writing to the same output file. Per-archive sitemap files are
replaced on rerun and removed when an archive produces no sitemap links.

The local sitemap-link pipeline writes one TSV file per Spark partition:

```text
target/commoncrawl-sitemaps/part-00000.sitemaps.tsv
```

Each local row contains four tab-separated fields: robots.txt file path,
robots.txt host directory, URI scheme inferred from the file name, and sitemap
URL. Local partition files are replaced on rerun.

The local sitemap filter reads `*.sitemaps.tsv` files produced by
`local-sitemaps` and writes grouped TSV files beneath the requested output
directory:

```text
target/filtered-sitemaps/country/ukraine/part-00000.sitemaps.tsv
target/filtered-sitemaps/country/russia/part-00000.sitemaps.tsv
target/filtered-sitemaps/country/united-kingdom/part-00000.sitemaps.tsv
target/filtered-sitemaps/language-region/uk-UA/part-00000.sitemaps.tsv
target/filtered-sitemaps/language-region/ru-RU/part-00000.sitemaps.tsv
target/filtered-sitemaps/language-region/en-GB/part-00000.sitemaps.tsv
target/filtered-sitemaps/language-region/unknown/part-00000.sitemaps.tsv
```

Each filtered row preserves the original four local sitemap fields and appends
country key, country name, matched suffix, and detected language-region. Country
matching uses the vendored suffix database in
`src/main/resources/sitemap-filter/country-suffixes.tsv`: Ukraine `.ua` and
`.укр`, Russia `.ru` and `.рф`, and United Kingdom `.uk`. The Cyrillic IDN
suffixes are normalized with punycode equivalents, and `.gb` is intentionally
not included because it is reserved.

Language-region detection is based only on sitemap URL host and path markers.
The filter recognizes Ukrainian markers such as `uk`, `uk-ua`, `ua`, and
`ua-uk`; Russian markers such as `ru` and `ru-ru`; and UK English markers such
as `en-gb`, `en-uk`, and `gb`. Rows without a marker are written to
`language-region/unknown`. The filter does not download sitemap files or infer
language from page content.

## How The Robots Pipeline Works

`CommonCrawlRobotsPipeline` is the main data pipeline behind the application.
It takes the parsed CLI configuration, downloads the Common Crawl manifest,
uses Spark to process each listed robots.txt WARC archive in parallel, and
writes one text file for each valid robots.txt response record it finds.

The pipeline starts by normalizing the manifest URL. A direct
`robotstxt.paths.gz` URL is used as-is. A sibling `wat.paths.gz` URL is
rewritten to `robotstxt.paths.gz`, which makes it convenient to copy either
kind of Common Crawl paths URL from the crawl index page.

The manifest is streamed to a temporary `.gz` file with sttp, then read through
`GZIPInputStream`. Empty lines and comments are ignored, and only archive paths
that contain `/robotstxt/` and end in `.warc.gz` are kept. Relative archive
paths are resolved against `https://data.commoncrawl.org/`; absolute paths are
used directly. When `--max-files N` is set, only the first `N` matching archive
paths are handed to Spark.

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
are handled, including stacked encodings. The decoded text is parsed with
`RobotsTxtParser`, and only captures with at least one valid user-agent group
and no parser warnings are written to the output directory. Invalid captures
are rejected and left off disk.

Output files are grouped by lowercased target host. The file name includes the
URI scheme, WARC capture date, and the first 16 hex characters of a SHA-256
digest over the target URI, capture date, and source archive path. This keeps
names readable while avoiding collisions between repeated captures.

Each archive task reports the number of valid files it saved, the number of
invalid files it rejected, or the failure it hit. After Spark collects the task
results, the driver prints a run summary with total saved and rejected counts.
If any archive failed to download or extract, the driver prints each failed
archive and raises an exception so the overall run exits unsuccessfully.

## How The Sitemaps Pipeline Works

`CommonCrawlSitemapsPipeline` reuses the same Common Crawl manifest handling,
archive download, retry/backoff, response decoding, and WARC parsing behavior as
the robots pipeline. It parses each matching `/robots.txt` response with
`RobotsTxtParser`; valid captures contribute their `Sitemap:` links, while
invalid captures are counted as rejected and do not produce output rows.

Each archive task writes its sitemap links to a deterministic
`archive-<hash>.sitemaps.tsv` file under the configured output directory and
logs the number of valid robots.txt files parsed, invalid files rejected, and
sitemap links saved. The driver prints total parsed, rejected, and saved-link
counts after Spark collects the task results.

`LocalRobotsSitemapsPipeline` accepts a directory that contains already saved
robots.txt files, such as `target/commoncrawl-robots`. The driver recursively
lists `.txt` files, Spark parses them in bounded partitions, and valid
robots.txt files contribute their distinct `Sitemap:` links to partition TSV
files. Invalid local robots.txt files are counted as rejected and do not
produce output rows.

`LocalSitemapsFilterPipeline` accepts a directory of local sitemap TSV files,
such as `target/commoncrawl-sitemaps`. The driver recursively lists
`*.sitemaps.tsv` files, Spark filters rows by the vendored country suffix
database, and matching rows are written once to a country group and once to a
language-region group. Country classification uses the robots host field first
and falls back to the sitemap URL host when the robots host is unknown or not in
the target suffix set.

## Build And Validation

Run sbt commands serially. Starting multiple sbt processes at the same time can
hit the sbt boot socket lock and fail with `ServerAlreadyBootingException`.

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true test
sbt -Dsbt.batch=true "run --help"
sbt -Dsbt.batch=true "run local-sitemaps target/commoncrawl-robots target/commoncrawl-sitemaps"
sbt -Dsbt.batch=true "run filter-sitemaps target/commoncrawl-sitemaps target/filtered-sitemaps"
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
  adds munit for tests; configures Spark Java module options; sets the
  `sbt run` heap; and builds `app.jar` with `sbt-assembly`.
- `src/main/scala/Main.scala`: application entry point.
- `src/main/scala/Cli.scala`: argument parsing and runtime defaults.
- `src/main/scala/SparkSessionFactory.scala`: Spark session construction,
  local-cluster driver binding, executor memory, Java module options, and
  Scala library classpath handling for executor RPC serialization.
- `src/main/scala/CommonCrawlRobotsArchiveSupport.scala`: shared Common Crawl
  manifest, archive download, retry/backoff, target filtering, and HTTP body
  decoding helpers.
- `src/main/scala/CommonCrawlRobotsPipeline.scala`: Spark parallel archive
  extraction, WARC parsing, robots.txt validation, and valid robots.txt file
  writes.
- `src/main/scala/CommonCrawlSitemapsPipeline.scala`: Spark pipeline that parses
  valid robots.txt captures and writes extracted sitemap links as TSV rows.
- `src/main/scala/LocalRobotsSitemapsPipeline.scala`: Spark pipeline that parses
  locally saved robots.txt files and writes extracted sitemap links as TSV rows.
- `src/main/scala/LocalSitemapsFilterPipeline.scala`: Spark pipeline that reads
  local sitemap TSV files and writes Ukraine, Russia, and UK grouped sitemap
  rows.
- `src/main/resources/sitemap-filter/country-suffixes.tsv`: vendored country
  suffix database for the sitemap filter.
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
