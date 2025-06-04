# spark/Dockerfile

FROM bitnami/spark:3.3

USER root

# Install dependencies
RUN apt-get update && apt-get install -y curl unzip wget openjdk-17-jdk

# Set Hudi and Scala version explicitly
ENV HUDI_VERSION=0.14.1
ENV SPARK_SCALA_VERSION=2.12

# Download Hudi Spark bundle
RUN wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_${SPARK_SCALA_VERSION}/${HUDI_VERSION}/hudi-spark3.3-bundle_${SPARK_SCALA_VERSION}-${HUDI_VERSION}.jar -P /opt/bitnami/spark/jars/

# Add MySQL connector (optional for future JDBC use)
RUN wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /opt/bitnami/spark/jars/

WORKDIR /app
