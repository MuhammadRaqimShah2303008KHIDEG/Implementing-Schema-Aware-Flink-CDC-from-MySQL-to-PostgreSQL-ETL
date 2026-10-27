# Flink CDC 3.6+ is built per Flink version: 3.6.0-1.20 needs Flink / PyFlink 1.20.x
FROM flink:1.20-java17

# Artifact suffix = Flink version. Newer CDC releases (e.g. 3.7.0-1.20) also work in principle; untested here.
ARG CDC_VERSION=3.6.0-1.20

USER root

# python3-dev + build-essential + a full JDK are needed on ARM64, where PyFlink's
# native dependency (pemja) may have to be compiled from source and needs the JNI headers.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Only ONE MySQL CDC jar, kept outside /opt/flink/lib on purpose:
# `python main.py` runs PyFlink's embedded mini-cluster, which does not read /opt/flink/lib.
# main.py loads this jar through env.add_jars(CDC_JAR).
# CDC uber jar does not ship mysql-connector-j (GPL). Debezium needs com.mysql.cj.jdbc.Driver.
RUN mkdir -p /opt/flink/usrlib && \
    wget -q -O /opt/flink/usrlib/flink-sql-connector-mysql-cdc.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.6.0-1.20/flink-sql-connector-mysql-cdc-3.6.0-1.20.jar" && \
    wget -q -O /opt/flink/usrlib/mysql-connector-j.jar \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"

WORKDIR /opt/flink/workspace

COPY requirements.txt .
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" \
    pip3 install --no-cache-dir -r requirements.txt
COPY .env .
COPY --chown=flink:flink main.py schema_pick.py pick_mysql_changes.py upsert.py ./

# Persistent state: schema snapshot + checkpoints
RUN mkdir -p /data/checkpoints && chown -R flink:flink /data /opt/flink/workspace
VOLUME /data

ENV CDC_JAR=/opt/flink/usrlib/flink-sql-connector-mysql-cdc.jar \
    MYSQL_JDBC_JAR=/opt/flink/usrlib/mysql-connector-j.jar \
    SCHEMA_SNAPSHOT_PATH=/data/schema_snapshot.json \
    CHECKPOINT_DIR=file:///data/checkpoints \
    PYTHONUNBUFFERED=1

USER flink

CMD ["python3", "main.py"]

