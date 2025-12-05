# VUT FIT - PDI 25/26
# Apache Spark Structured Streaming
## Instructions (Standalone) 
tested in Ubuntu 24.04 WSL
### Prerequisites
1. java 17: `sudo apt install openjdk-17-jre-headless`
2. python 3.12.3: `sudo apt install python3`
3. docker 28.2.2: `sudo apt install docker.io docker-compose -y`
    
    3.1. add user to docker group: `sudo usermod -aG docker $USER`
    
    3.2. log in to new group: `newgrp docker`
### How to run
1. run kafka: `docker run -d --name kafka -p 9092:9092 apache/kafka:latest`
2. create python venv
3. activate python venv
4. `pip install -r requirements.txt`
5. `python3 sse_to_kafka.py`
6. in a new terminal: `python3 pyspark_consumer.py [ARGS]`

## Instructions (docker-submit using Docker)
### Prerequisites
1. docker 28.2.2: `sudo apt install docker.io docker-compose -y`
### How to run
1. `docker-compose up`

    This runs kafka broker, kafka producer, and spark consumer running the `server_counts` query for 30 seconds.

    The output is stored in `consumer_output`.

2. To run other queries, in a new terminal, run: `docker-compose run consumer [ARGS]`

### Stopping
1. `docker-compose down`
