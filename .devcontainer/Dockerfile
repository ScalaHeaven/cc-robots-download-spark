FROM eclipse-temurin:21-jdk-noble

ARG USERNAME=vscode
ARG NODE_MAJOR=26
ARG COURSIER_VERSION=v2.1.25-M24
ARG SPARK_VERSION=3.5.1

ENV SPARK_HOME=/opt/spark
ENV SPARK_SCALA_VERSION=2.13

# Base OS tools used by git, VS Code, NodeSource, and setup scripts.
RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      git \
      gnupg \
      gzip \
      openjdk-21-source \
      openssh-client \
      sudo \
    && ln -sf /usr/lib/jvm/openjdk-21/lib/src.zip /opt/java/openjdk/lib/src.zip \
    && rm -rf /var/lib/apt/lists/*

# Node.js is installed from NodeSource so Codex can be installed with npm.
RUN install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
      | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODE_MAJOR}.x nodistro main" \
      > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g @openai/codex \
    && rm -rf /var/lib/apt/lists/*

# The devcontainer runs as a non-root sudo user, matching VS Code defaults.
RUN useradd --create-home --shell /bin/bash "${USERNAME}" \
    && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/${USERNAME}" \
    && chmod 0440 "/etc/sudoers.d/${USERNAME}" \
    && mkdir -p "/home/${USERNAME}/.sbt" "/home/${USERNAME}/.cache/coursier" \
    && chown -R "${USERNAME}:${USERNAME}" "/home/${USERNAME}/.sbt" "/home/${USERNAME}/.cache"

# Install Coursier, then use it for Scala tools and an sbt wrapper.
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
    COURSIER_CACHE="/home/${USERNAME}/.cache/coursier" cs install scala3-compiler scala-cli metals-mcp --install-dir /usr/local/bin; \
    mkdir -p "${SPARK_HOME}/jars"; \
    COURSIER_CACHE="/home/${USERNAME}/.cache/coursier" cs fetch "org.apache.spark:spark-sql_2.13:${SPARK_VERSION}" --classpath \
      | tr ':' '\n' \
      | while IFS= read -r jar_path; do cp "$jar_path" "${SPARK_HOME}/jars/"; done; \
    printf '%s\n' "spark-sql_${SPARK_SCALA_VERSION}:${SPARK_VERSION}" > "${SPARK_HOME}/.spark-version"; \
    printf '%s\n' '#!/usr/bin/env sh' 'export COURSIER_CACHE="${COURSIER_CACHE:-/home/vscode/.cache/coursier}"' 'exec /usr/local/bin/cs launch sbt -- "$@"' > /usr/local/bin/sbt; \
    chmod +x /usr/local/bin/sbt; \
    chown -R "${USERNAME}:${USERNAME}" "/home/${USERNAME}/.cache" "${SPARK_HOME}" || true

USER ${USERNAME}

# Fail the build early if the installed language-server bridge is broken.
RUN metals-mcp --version

# Warm the most expensive sbt downloads used by the template project.
RUN COURSIER_CACHE="/home/${USERNAME}/.cache/coursier" \
    cs fetch org.scala-sbt:sbt-launch:1.12.11 io.get-coursier.sbt:sbt-runner:0.2.0 >/dev/null
