# VUT FIT - PDI 25/26
## Instructions (tested in Ubuntu 24.04 WSL)
### Prerequisites
1. java 17: `sudo apt install openjdk-17-jre-headless`
### How to run
1. `sudo apt install docker.io docker-compose -y`
2. `sudo usermod -aG docker $USER`
3. `newgrp docker`
4. `docker run -d --name kafka -p 9092:9092 apache/kafka:latest`
5. create python venv
6. activate python venv
7. `pip install -r requirements.txt`
8. `python3 sse_to_kafka.py`
9. in a new terminal: `python3 pyspark_consumer.py`