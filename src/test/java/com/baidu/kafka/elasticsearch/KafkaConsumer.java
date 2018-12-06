package com.baidu.kafka.elasticsearch;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.baidu.kafka.elasticsearch.ConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer {
	private ConsumerConnector consumer;
	private String topic;
	private ExecutorService executor;
	private Properties props = new Properties();
	private KafkaStream stream;
	private String esHost;
	private int bulkSize;
	private String esCluster;
	private List<KafkaStream<byte[], byte[]>> streams;
	protected static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

	public KafkaConsumer(String zookeeper, String groupId, String topic, String esHost, int bulkSize,
			String esCluster) {
		LOG.info("kafka init");
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
		this.esHost = esHost;
		this.bulkSize = bulkSize;
		this.esCluster = esCluster;
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", ConfigFile.ZK_SESSION_TIMEOUT_MS);
		props.put("zookeeper.sync.time.ms", ConfigFile.ZK_SYNC_TIME_MS);
		props.put("auto.commit.interval.ms", ConfigFile.AUTO_COMMIT_INTERVAL_MS);
		props.put("consumer.timeout.ms", ConfigFile.CONSUMER_TIMEOUT_MS);
		props.put("auto.offset.reset", ConfigFile.AUTO_OFFSET_RESET);
		props.put("rebalance.backoff.ms", ConfigFile.REBALANCE_MS);
		ConsumerConfig config = new ConsumerConfig(props);
		return config;
	}

	public void run(int numThreads) throws IOException {
		LOG.info("Run!");
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numThreads));
		Properties properties = new Properties();  
		properties.put("zookeeper.connect", "192.168.79.132:2181");//����zk  
		properties.put("group.id", "group1");// ����Ҫʹ�ñ�������ƣ� ��������ߺ������߶���ͬһ�飬���ܷ���ͬһ���ڵ�topic����  
		properties.put("auto.offset.reset", "smallest");
		
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
		Map<String, Integer> map = new HashMap<>();
		map.put("test-elasticsearch-sink", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumermap = consumer.createMessageStreams(map );
		List<KafkaStream<byte[], byte[]>> streams = consumermap.get(topic);
		 executor = Executors.newFixedThreadPool(new Integer(numThreads));
         int threadNumber = 0;
         for (KafkaStream<?, ?> stream : streams) {
               executor.submit(new ElasticsearchInsertd(stream, esHost, threadNumber, bulkSize, esCluster));
               threadNumber++;
         }
	}
	
	public static void main(String[] args) {
		try {
			
			String zkHost = "192.168.79.132";
			String zkHostPort = zkHost + ":" + "2181";
			String esHost = "192.168.79.129";
			String topic = "test-elasticsearch-sink";
			String groupId = "group2";
			int threads = 3;
			int bulksize = 3000;
			String esCluster = "my-application";
			LOG.info("Start!");
			LOG.info("topic is: " + topic);
			KafkaConsumer kComsumer = new KafkaConsumer(zkHostPort, groupId, topic, esHost, bulksize, esCluster);
			kComsumer.run(threads);
			LOG.info("End!");
		} catch (Exception e) {
			LOG.info("{} " + e.getMessage());
		}
	}
}
