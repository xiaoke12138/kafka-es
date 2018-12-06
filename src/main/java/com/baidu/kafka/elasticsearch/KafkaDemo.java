package com.baidu.kafka.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaDemo {
	@Test
	public void Consumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "10.170.130.133:2181");// ����zk
		properties.put("group.id", "group2");// ����Ҫʹ�ñ�������ƣ� ��������ߺ������߶���ͬһ�飬���ܷ���ͬһ���ڵ�topic����
		properties.put("auto.offset.reset", "smallest");

		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(properties));
		Map<String, Integer> map = new HashMap<>();
		map.put("base_yt_app", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> stream = consumer.createMessageStreams(map);
		List<KafkaStream<byte[], byte[]>> list = stream.get("base_yt_app");
		KafkaStream<byte[], byte[]> ks = list.get(0);
		ConsumerIterator<byte[], byte[]> it = ks.iterator();
		while (it.hasNext()) {
			byte[] data = it.next().message();
			System.out.println(new String(data));
		}
	}

	@Test
	public void producer() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.79.132:2181");
		props.put("zookeeper.connect", "192.168.79.132:2181");
        ProducerConfig config = new ProducerConfig(props);  
        Producer<String, String> producer = new Producer<String, String>(config);    
        producer.send(new KeyedMessage<String,String>("test-elasticsearch-sink", "192.168.79.132",
				"{\"account_number\":18,\"balance\":4180,\"firstname\":\"Dale\",\"lastname\":\"Adams\",\"age\":33,\"gender\":\"M\",\"address\":\"467 Hutchinson Court\",\"employer\":\"Boink\",\"email\":\"daleadams@boink.com\",\"city\":\"Orick\",\"state\":\"MD\"}"));
	}

	public static void main(String[] args) {
		long endTime = System.currentTimeMillis() / 1000;
		System.out.println(endTime);
		String jsontest="{\"account_number\":18,\"balance\":4180,\"firstname\":\"Dale\",\"lastname\":\"Adams\",\"age\":33,\"gender\":\"M\",\"address\":\"467 Hutchinson Court\",\"employer\":\"Boink\",\"email\":\"daleadams@boink.com\",\"city\":\"Orick\",\"state\":\"MD\"}\r\n" + 
				"\r\n" + 
				"";
		System.out.println(isjson(jsontest));
	}

	public static  boolean isjson(String json) {
		ObjectMapper obm = new ObjectMapper();
		try {
			obm.readTree(json);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
