echo "###############################################"
echo "Killing Flume..."
echo "###############################################"
kill `jps | grep "Application" | cut -d " " -f 1`

echo "###############################################"
echo "Killing python script..."
echo "###############################################"
pkill python

echo "###############################################"
echo "Stopping Kafka..."
echo "###############################################"
kafka-server-stop.sh

echo "###############################################"
echo "Stopping ZooKeeper server..."
echo "Using config file: conf/zoo.cfg"
echo "###############################################"
zkServer.sh stop config/zoo.cfg 

rm nohup.out
rm logs/tweet.log

### To delete a topic:
### 1. stop kafka server
### 2. delete topic directory in kafka-logs
### 3. connect to zookeeper shell: zookeeper-shell.sh localhost:2181
### 4. list topics: ls /brokers/topics
### 5. delete topic: rmr /brokers/topics/<topic>
### 6. exit shell