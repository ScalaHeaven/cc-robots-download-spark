ARG SCALA_TARGET_VERSION=3.8.3
ARG COURSIER_VERSION=v2.1.25-M24
ARG SPARK_VERSION=3.5.1

FROM eclipse-temurin:21-jdk AS build

ARG COURSIER_VERSION
ARG SPARK_VERSION

WORKDIR /app

# Install the small set of OS tools needed to download Coursier.
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gzip \
    && rm -rf /var/lib/apt/lists/*

# Install Coursier, then use it to install sbt.
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) coursier_arch="x86_64-pc-linux" ;; \
      arm64) coursier_arch="aarch64-pc-linux" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fL "https://github.com/coursier/coursier/releases/download/${COURSIER_VERSION}/cs-${coursier_arch}.gz" \
      | gzip -d > /usr/local/bin/cs; \
    chmod +x /usr/local/bin/cs; \
    cs install sbt --install-dir /usr/local/bin

# Local Spark cluster mode launches executor JVMs that need a Spark home.
RUN set -eux; \
    mkdir -p /opt/spark/jars; \
    cs fetch "org.apache.spark:spark-sql_2.13:${SPARK_VERSION}" --classpath \
      | tr ':' '\n' \
      | while IFS= read -r jar_path; do cp "$jar_path" /opt/spark/jars/; done

# Copy build metadata first so Docker can cache dependency downloads.
COPY project ./project
COPY build.sbt ./
COPY .scalafmt.conf ./
RUN sbt -Dsbt.batch=true update

# Copy application sources last; this is the layer that changes most often.
COPY src ./src
RUN sbt -Dsbt.batch=true assembly

FROM eclipse-temurin:21-jre

WORKDIR /app

ARG SCALA_TARGET_VERSION

ENV SPARK_HOME=/opt/spark
ENV SPARK_SCALA_VERSION=2.13

COPY --from=build /app/target/scala-${SCALA_TARGET_VERSION}/app.jar ./app.jar
COPY --from=build /opt/spark /opt/spark

ENTRYPOINT ["java", "-Xmx4g", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", "--add-opens=java.base/sun.security.action=ALL-UNNAMED", "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED", "-cp", "/opt/spark/jars/*:/app/app.jar", "Main"]
