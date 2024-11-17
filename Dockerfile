FROM spark:latest
WORKDIR /opt/spark/work-dir

USER root

# Install the application dependencies
RUN pip install graphframes