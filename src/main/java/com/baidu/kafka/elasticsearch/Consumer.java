package com.baidu.kafka.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer {
	private ConsumerConnector consumer;
	private String topic;
	private ExecutorService executor;
	private Properties props = new Properties();
	private KafkaStream stream;
	private String esHost;
	private int bulkSize;
	private String esCluster;
	private List<KafkaStream<byte[], byte[]>> streams;
	protected static Logger LOG = LoggerFactory.getLogger(Consumer.class);

	public Consumer(String zookeeper, String groupId, String topic, String esHost, int bulkSize,
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
		properties.put("zookeeper.connect", "10.170.130.133:2181");//����zk  
		properties.put("group.id", "group1");// ����Ҫʹ�ñ�������ƣ� ��������ߺ������߶���ͬһ�飬���ܷ���ͬһ���ڵ�topic����  
		properties.put("auto.offset.reset", "smallest");
		
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
		Map<String, Integer> map = new HashMap<>();
		map.put("base_yt_app", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumermap = consumer.createMessageStreams(map );
		List<KafkaStream<byte[], byte[]>> streams = consumermap.get(topic);
/*		KafkaStream<byte[], byte[]> kafkaStream = consumermap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
		System.out.printf("it.hasNext()",it.hasNext());
		while (it.hasNext()) {
			byte[] data = it.next().message();
			System.out.println(new String(data));
		}*/
		 executor = Executors.newFixedThreadPool(new Integer(numThreads));
         int threadNumber = 0;
         for (KafkaStream<?, ?> stream : streams) {
        	 System.out.println("consumer进stream");
               executor.submit(new ElasticsearchInsertd(stream, esHost, threadNumber, bulkSize, esCluster));
               threadNumber++;
         }
	}
	
	public static void main(String[] args) {
		try {
			
			String zkHost = "10.170.130.133";
			String zkHostPort = zkHost + ":" + "2181";
			String esHost = "10.170.130.154";
			String topic = "base_yt_app";
			String groupId = "group2";
			int threads = 3;
			int bulksize = 3000;
			String esCluster = "my-application";
			LOG.info("Start!");
			LOG.info("topic is: " + topic);
			Consumer kComsumer = new Consumer(zkHostPort, groupId, topic, esHost, bulksize, esCluster);
			kComsumer.run(threads);
			LOG.info("End!");
		} catch (Exception e) {
			LOG.info("{} " + e.getMessage());
		}
	}
}
