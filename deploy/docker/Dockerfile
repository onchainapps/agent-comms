# agent-comms server container (RFC-001 §9 single-writer applies inside too:
# exactly ONE container per volume — never scale this service, never run two
# containers against the same COMMS_HOME volume; SQLite WAL + the flock latch
# will refuse the second writer).
#
# build:  docker build -f deploy/docker/Dockerfile -t agent-comms .
# run:    see deploy/docker/docker-compose.yml
FROM oven/bun:1.3.14-slim

# sqlite3: deploy/backup.sh (.backup) inside the container; ca-certs for
# outbound fetches if COMMS_URL ever points at TLS.
RUN apt-get update \
 && apt-get install -y --no-install-recommends sqlite3 ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# non-root runtime user; /data holds COMMS_HOME (db + messages mirror).
RUN useradd -r -u 10001 -d /data comms \
 && mkdir -p /data \
 && chown -R comms:comms /data /app

USER comms
ENV COMMS_HOME=/data \
    COMMS_HOST=0.0.0.0 \
    COMMS_PORT=8700 \
    BUN_ENV=production

EXPOSE 8700
VOLUME ["/data"]

RUN chmod +x deploy/*.sh deploy/docker/entrypoint.sh

# tini: PID 1 reaping + signal forwarding (SIGTERM ⇒ clean close ⇒ WAL
# checkpoint, §9). entrypoint.sh: first-run bootstrap + flock single-writer.
# Healthcheck hits /health on loopback.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD bun -e 'fetch("http://127.0.0.1:8700/health").then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))'

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/deploy/docker/entrypoint.sh"]
