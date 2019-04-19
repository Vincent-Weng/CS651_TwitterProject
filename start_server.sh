echo "###############################################"
echo "Starting ZooKeeper server..."
echo "Using config file: conf/zoo.cfg"
echo "###############################################"
zkServer.sh start config/zoo.cfg

echo "###############################################"
echo "Starting Kafka..."
echo "###############################################"
kafka-server-start.sh -daemon config/server.properties&

echo "###############################################"
echo "Waiting for Kafka server..."
echo "###############################################"

sleep 5

touch logs/tweet.log

if jps | grep -q ".*Kafka";
then
    echo "###############################################"
    echo "Starting Flume..."
    echo "###############################################"
    nohup flume-ng agent --name exec-memory-kafka --conf ./config --conf-file ./config/flume_kafka.conf -Dflume.root.logger=INFO,console&
else
    echo "Kafka server not available!"
fi

# Create topic
# kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterStreaming

# List topics
# kafka-topics.sh --list --zookeeper localhost:2181

# Console producer:
# kafka-console-producer.sh --broker-list localhost:9092 --topic twitterStreaming

# Console consumer:
# kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitterStreaming
