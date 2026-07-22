# Java 17 + Ubuntu — no apt-get needed, avoids corporate proxy issues
FROM eclipse-temurin:17-jre-jammy

# uv manages Python itself — install the binary directly
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_PYTHON=3.11
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install project dependencies + dev group (includes ipykernel)
RUN uv sync --frozen --group dev

# Copy source code
COPY . .

EXPOSE 8888 4040

CMD ["uv", "run", "jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--notebook-dir=/app/spark/jobs"]
