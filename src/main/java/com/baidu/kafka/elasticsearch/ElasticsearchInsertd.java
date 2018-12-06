package com.baidu.kafka.elasticsearch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ElasticsearchInsertd implements Runnable {
	private int threadNum;
	private int bulkSize;
	private KafkaStream stream;
	private int threadNumber;
	private String kafkaMsg;
	protected String productName;
	protected String serviceName;
	protected TransportClient client;
	protected String indexName;
	protected String typeName;
	protected ArrayList<JSONObject> jsonList = new ArrayList<JSONObject>();
	protected Map<String, NodeInfo> nodesMap = new HashMap<>();
	protected String elasticSearchHost = ConfigFile.ES_HOSTS;
	protected Integer elasticSearchPort = ConfigFile.ES_PORT;
	protected String elasticSearchCluster = ConfigFile.ES_CLUSTER_NAME;
	protected static Logger LOG = LoggerFactory.getLogger(ElasticsearchInsertd.class);

	public  ElasticsearchInsertd(KafkaStream stream, String esHost, int threadNum, int bulkSize, String esCluster) throws IOException {
		this.stream = stream;
		this.threadNum = threadNum;
		this.bulkSize = bulkSize;
		elasticSearchCluster = esCluster;
		elasticSearchHost = esHost;
		LOG.info("进入ElasticsearchInsert");
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		client = new PreBuiltTransportClient(settings).addTransportAddresses(
				new TransportAddress(InetAddress.getByName(elasticSearchHost), elasticSearchPort));
        LOG.info("client==" + client.toString());
        //测试是否能够写数据
/*		client.prepareIndex("company", "employee", "1")
		.setSource(XContentFactory.jsonBuilder().startObject().field("name", "marry").field("age", 35)
				.field("position", "technique manager").field("country", "china")
				.field("join_date", "2017-01-01").field("salay", 12000).endObject())
		.get();*/
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

	public boolean insertES(ArrayList<JSONObject> jsonAry) {
		String document = null;
		try {
			LOG.info("client==里的状态" + client.toString()+elasticSearchHost+elasticSearchPort);
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (JSONObject json : jsonAry) {
				indexName = "base_app";
				typeName = "appnews";
				document = json.toString();
				System.out.println("document"+document);
				System.out.println("json"+json);
				XContentParser parser = XContentFactory.xContent(XContentType.JSON). createParser(NamedXContentRegistry.EMPTY, document);
				bulkRequest.add(this.client.prepareIndex(indexName, typeName).setSource(parser.map()));
			}
			BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
			return true;
		} catch (Exception e) {
			LOG.info("Unable to index Document[ " + document + "], Type[" + typeName + "], Index[" + indexName + "]",
					e);
			return false;
		}
	}

	@Override 
	public void run() {
		LOG.info("进入run");
		long startTime = System.currentTimeMillis() / 1000;
		ConsumerIterator<byte[], byte[]> msgStream = stream.iterator();
		while(msgStream.hasNext()){
			/*byte[] data = msgStream.next().message();
			System.out.println("run里面收到的串"+new String(data));*/
			//System.out.println("run里的状态"+client.toString()+elasticSearchHost+elasticSearchPort);
			try {
				kafkaMsg = new String(msgStream.next().message(), "UTF-8");
				System.out.println("kafkaMsg"+kafkaMsg);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			JSONObject json=null;
			if(isjson(kafkaMsg)) {
				json = new JSONObject(kafkaMsg);
				System.out.println("kafkajson"+json);
			}else {
				System.out.println("无法转换json");
				continue;
			}
			System.out.println("run json"+json);
			jsonList.add(json);
			long endTime = System.currentTimeMillis() /  1000;
            //收一定的条数然后break
			/*			if (jsonList.size()>= 5)
				break;
		}
		if (insertES(jsonList)) {
			jsonList.clear();*/
			//不限制条数的写法
			if (jsonList.size()>=3) {
				insertES(jsonList);
				jsonList.clear();
			}
		}
	}
}
