package com.baidu.kafka.elasticsearch;
import java.io.IOException;
import java.net.InetAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;


public class GetConnection {

	public final static String HOST = "192.168.79.129";
	// http请求的端口是9200，客户端是9300
	public final static int PORT = 9300;

	/**
	 * getConnection:(获取es连接).
	 * 
	 * @author xbq Date:20180710
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "resource", "unchecked" })
	public static TransportClient getConnection() throws Exception {
		// 设置集群名称
		Settings settings = Settings.builder().put("cluster.name", "my-application").build();
		// 创建client
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));

		// client.close();
		return client;
	}

	public static void main(String[] args) throws Exception {
		TransportClient client = getConnection();
		System.out.println("client==" + client.toString());
		// 准备数据
		preparDate(client);                   
		client.close();
	}
	

	public static void preparDate(TransportClient client) throws IOException {
		client.prepareIndex("company", "employee", "1")
				.setSource(XContentFactory.jsonBuilder().startObject().field("name", "marry").field("age", 35)
						.field("position", "technique manager").field("country", "china")
						.field("join_date", "2017-01-01").field("salay", 12000).endObject())
				.get();

		client.prepareIndex("company", "employee", "2")
				.setSource(XContentFactory.jsonBuilder().startObject().field("name", "marry").field("age", 35)
						.field("position", "technique manager").field("country", "china")
						.field("join_date", "2017-01-01").field("salary", 12000).endObject())
				.get();

		client.prepareIndex("company", "employee", "3")
				.setSource(XContentFactory.jsonBuilder().startObject().field("name", "tom").field("age", 32)
						.field("position", "senior technique software").field("country", "china")
						.field("join_date", "2016-01-01").field("salary", 11000).endObject())
				.get();

		client.prepareIndex("company", "employee", "4")
				.setSource(XContentFactory.jsonBuilder().startObject().field("name", "jen").field("age", 25)
						.field("position", "junior finance").field("country", "usa").field("join_date", "2016-01-01")
						.field("salary", 7000).endObject())
				.get();

		client.prepareIndex("company", "employee", "5")
				.setSource(XContentFactory.jsonBuilder().startObject().field("name", "mike").field("age", 37)
						.field("position", "finance manager").field("country", "usa").field("join_date", "2015-01-01")
						.field("salary", 15000).endObject())
				.get();
		System.out.println("数据准备完成");
	}
}