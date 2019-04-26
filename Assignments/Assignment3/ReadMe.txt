CS6350  Assignment 3
Jiadao Zou
jxz172230
==============================
==============================

PART 1
==============================
PREPARATION:
You may want to specific the JAVA version of Spark-submit
    vim /usr/local/spark/conf/spark-env.sh
Add the following sentence to it
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk/jre/
Also change the JAVA configuration of spark-shell
    vim /usr/local/spark/bin/spark-shell
Add the following sentence to it
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk/jre/

==============================

Start the Server
    ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/zookeeper-server-start.sh config/zookeeper.properties
    ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-server-start.sh config/server.properties
List all topics
    ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
            '''
            Create topics instream and outstream
                bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic instream
                bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic outstream
            Send message
                bin/kafka-console-producer.sh --broker-list localhost:9092 --topic instream
            Set up Consumer
                bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic instream --from-beginning
                bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outstream --from-beginning
            Run the example of teacher:
                cd /Downloads/cs6350/kafka_example/kafka/kafka/target/scala-2.11/
                spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class KafkaExample kafka-assembly-0.1.jar topicA topicB
            Test version
                cd ~/Code/Github/CS6350-BigData/Assignments/Assignment3/kafka/target/scala-2.11
                spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class Ttest kafka-assembly-0.1.jar Centaurus universe galaxy 
            '''

Run the Elasticsearch
    ~/Downloads/cs6350/elasticsearch-7.0.0/bin/elasticsearch
Run the Kibana
    ~/Downloads/cs6350/kibana-7.0.0-linux-x86_64/bin/kibana
Run the Logstash
    ~/Downloads/cs6350/logstash-7.0.0/bin/logstash -f logstash-simple.conf
Run the Twitter getter
    cd ~/Code/Github/CS6350-BigData/Assignments/Assignment3/kafka/target/scala-2.11/
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class KafkaTwitter kafka-assembly-0.1.jar Centaurus universe galaxy
    
