CS6350  Assignment 3
Jiadao Zou
jxz172230
==============================
==============================

PART 1 (includes brief summary)
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

1.Start the Servers
    cd ~/Downloads/cs6350/kafka_2.12-2.2.0/ 
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
List all topics
    ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
            '''
            Create topics instream and outstream
                ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic instream
                ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic outstream
            Send message
                ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic instream
2.Set up Consumer
    ~/Downloads/cs6350/kafka_2.12-2.2.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter-index --from-beginning
            Run the example of teacher:
                cd /Downloads/cs6350/kafka_example/kafka/kafka/target/scala-2.11/
                spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class KafkaExample kafka-assembly-0.1.jar topicA topicB
            Test version
                cd ~/Code/Github/CS6350-BigData/Assignments/Assignment3/kafka/target/scala-2.11
                spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class Ttest kafka-assembly-0.1.jar Centaurus universe galaxy 
            '''
3.Run the Twitter getter
    cd ~/Code/Github/CS6350-BigData/Assignments/Assignment3/kafka/target/scala-2.11/
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 --class KafkaTwitter kafka-assembly-0.1.jar Centaurus universe galaxy
4.Run the Elasticsearch
    ~/Downloads/cs6350/elasticsearch-7.0.0/bin/elasticsearch
5.Run the Kibana
    ~/Downloads/cs6350/kibana-7.0.0-linux-x86_64/bin/kibana
6.Run the Logstash
    vim ~/Downloads/cs6350/logstash-7.0.0/logstash-simple.conf
    cd ~/Downloads/cs6350/logstash-7.0.0/
    bin/logstash -f logstash-simple.conf

Check your JSON from Elasticsearch
    curl -H 'Content-Type: application/json' -X GET http://localhost:9200/twitter/_search\?pretty
7.Visulizing
    http://localhost:5601
8.The results, please see the plots (I don't have enough time to wait for streaming hourly data for Negative and Postive Topic I mentioned below cause it is near to the DDL right now, thus I could only provide plots of serveral mins)
9.Conclusion:
    - As we could see, no matter what time, for the sepcific Netural topic I searched [Centaurus,Universe,Galaxy], seems the NLP sentiment analysis tool deduces more Negative opinion > Postive > Netural. 
    - For the Negative topic likes [NotreDame, Paris, Fire, Kill], all the time intervals, the tweets are mostly negative and very small part of them are saw as Postive. The NLP tasks at this time performs well.
    - For the Postive topic likes [Happiness, Optimism, Cheerful], all the time intervals, the Negative tweets are more than Postive ones, that is not good.
    So, current method tend to have a more "Negative" point of view, I think it may due to that I didn't filter put some Emoji text or other special kind of characters (like @others, location and other words) which would definitely influnece the performance.
    
    
PART 2
==============================
Uploaded the source data file and jar file into one folder in AWS then create the steps as the following commands
    spark-submit --deploy-mode cluster --class "TwitterQ2" [path of jar file] [path of source dataset] [path of output folder]
    spark-submit --deploy-mode cluster --class "TwitterQ2" 
    s3://cs6350bigdata/ass3/kafka/target/scala-2.11/kafka_2.11-0.1.jar
    s3://cs6350bigdata/ass3/Tweets.csv
    s3://cs6350bigdata/ass3/q2
