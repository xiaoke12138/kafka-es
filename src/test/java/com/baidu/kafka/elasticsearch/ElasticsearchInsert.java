package com.baidu.kafka.elasticsearch;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.lang.String;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Integer;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;

import org.json.JSONObject;
import org.json.JSONArray;

import com.baidu.kafka.elasticsearch.ConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class ElasticsearchInsert implements Runnable {
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
	protected static Logger LOG = LoggerFactory.getLogger(ElasticsearchInsert.class);

	public ElasticsearchInsert(KafkaStream stream, String esHost, int threadNum, int bulkSize, String esCluster)
			throws UnknownHostException {
		this.stream = stream;
		this.threadNum = threadNum;
		this.bulkSize = bulkSize;
		elasticSearchCluster = esCluster;
		elasticSearchHost = esHost;
		LOG.info("进入ElasticsearchInsert");
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 创建client
		/*
		 * Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name",
		 * elasticSearchCluster).build(); client = new
		 * TransportClient(settings).addTransportAddress(new
		 * InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
		 */
		TransportClient client = new PreBuiltTransportClient(settings).addTransportAddresses(
				new TransportAddress(InetAddress.getByName(elasticSearchHost), elasticSearchPort));
        LOG.info("创建连接完毕");
        LOG.info("client==" + client.toString());
		/*NodesInfoResponse response = client.admin().cluster().nodesInfo(new NodesInfoRequest().timeout("6000"))
				.actionGet();
		nodesMap = response.getNodesMap();
		for (String k : nodesMap.keySet()) {
			if (!elasticSearchHost.equals(nodesMap.get(k).getHostname())) {
				// client.addTransportAddress(new
				// InetSocketTransportAddress(nodesMap.get(k).getHostname(),
				// elasticSearchPort));
				client.addTransportAddress(new TransportAddress(InetAddress.getByName(nodesMap.get(k).getHostname()), elasticSearchPort));	
			}
		}*/
		LOG.info("init es");
	}

	public boolean insertES(ArrayList<JSONObject> jsonAry) {
		String document = null;
		try {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (JSONObject json : jsonAry) {
				/*
				 * log filter to json productName = json.getString("product"); serviceName =
				 * json.getString("service"); long insertTime = System.currentTimeMillis(); long
				 * eventTime = json.getLong("event_time"); //long cacheTime =
				 * json.getLong("cache_time"); json.put("insert_time", insertTime);
				 * SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); String
				 * dateTime = sf.format(eventTime); String dateTimeYMD[] = dateTime.split(" ");
				 * indexName = "aqueducts_" + productName + "_" + dateTimeYMD[0]; typeName =
				 * serviceName;
				 */
				SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String dateTime = sf.format(System.currentTimeMillis());
				String dateTimeYMD[] = dateTime.split(" ");
				indexName = "clog_" + dateTimeYMD[0];
				typeName = "jpaas";
				document = json.toString();
				bulkRequest.add(this.client.prepareIndex(indexName, typeName).setSource(document));
			}
			BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
			return true;
		} catch (Exception e) {
			LOG.info("Unable to index Document[ " + document + "], Type[" + typeName + "], Index[" + indexName + "]",
					e);
			return false;
		}
	}

	public void run() {
		LOG.info("Inert ElasticSearch");
		LOG.info("thread num: " + threadNum);
		while (true) {
			ConsumerIterator<byte[], byte[]> msgStream = stream.iterator();
			long startTime = System.currentTimeMillis() / 1000;
			int countPv = 0;
			try {
				while (msgStream.hasNext()) {
					byte[] data = msgStream.next().message();
					System.out.println("run里面收到的串"+new String(data));
					 System.out.println("kafka msg-----");
					kafkaMsg = new String(msgStream.next().message(), "UTF-8");
				      System.out.println(kafkaMsg);
					JSONObject json = new JSONObject(kafkaMsg);
					//  json type:{message:[log1,log2],tags:[tag1,tag2]}
					JSONArray messageAry = json.getJSONArray("message");
					for (int i = 0; i < messageAry.length(); i++) {
						String messageStr = messageAry.getString(i);
						JSONObject jsonb = new JSONObject();
						jsonb.put("message", messageStr);
						// logstash need version and timestamp
						jsonb.put("@version", "1");
						SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
						jsonb.put("@timestamp", sf.format(System.currentTimeMillis()));

						jsonList.add(jsonb);
					}
					// log filter to json
					// jsonList.add(json);
					int listSize = jsonList.size();
					long endTime = System.currentTimeMillis() / 1000;
					countPv++;
					if (((endTime - startTime) >= ConfigFile.INDEX_INTERVAL) || listSize >= bulkSize)
						break;
				}
				if (insertES(jsonList)) {
					LOG.info("pv: " + countPv);
					countPv = 0;
					for (String k : nodesMap.keySet()) {
						LOG.info(k + ":" + nodesMap.get(k).getHostname());
					}
					jsonList.clear();
				}
			} catch (Exception e) {
				LOG.info("failed to construct index request " + e.getMessage());
			}
		}
	}
}
