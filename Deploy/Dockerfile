# ---- Stage 1: Build JAR ----
FROM maven:3.9.6-eclipse-temurin-22 AS builder
WORKDIR /app
COPY ../pom.xml .
RUN mvn dependency:go-offline -B
COPY ../src ./src
RUN mvn clean package -DskipTests

# ---- Stage 2: Runtime with Cron ----
FROM eclipse-temurin:22-jre
# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install cron
RUN apt-get update && apt-get install -y cron tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=America/New_York

WORKDIR /app
COPY --from=builder /app/target/barchart.jar /app/barchart.jar

# Accept build arg to choose which crontab to use (without "root" column)
ARG CRONTAB_FILE
COPY ${CRONTAB_FILE} /app/crontab.txt

# Install crontab for default user
RUN crontab /app/crontab.txt

# Run cron in foreground
CMD ["cron", "-f"]
