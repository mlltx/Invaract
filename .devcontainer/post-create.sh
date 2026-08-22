#!/bin/bash
set -e

echo "=== Spark Plugin Dev Environment Setup ==="

# Install sbt
echo "Installing sbt..."
curl -fsSL https://github.com/coursier/coursier/releases/download/v2.1.9/cs-x86_64-pc-linux.gz | gzip -d > cs
chmod +x cs
./cs install sbt
rm cs

# Install Spark (for spark-submit)
echo "Installing Apache Spark..."
SPARK_VERSION="3.5.1"
HADOOP_VERSION="3"
SPARK_DIST="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_DIST}"

if [ ! -d "/opt/spark" ]; then
  mkdir -p /opt/spark
  cd /opt
  wget -q "$SPARK_URL" -O spark.tgz
  tar -xzf spark.tgz
  mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" spark
  rm spark.tgz
  cd -
fi

# Set environment variables
cat >> ~/.bashrc << 'EOF'
export SPARK_HOME="/opt/spark"
export PATH="$SPARK_HOME/bin:$PATH"
EOF

source ~/.bashrc

# Verify installations
echo "Verifying installations..."
java -version
sbt --version
spark-submit --version

echo "=== Setup Complete ==="
echo "Ready for Spark plugin development!"
