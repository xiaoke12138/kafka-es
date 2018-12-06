package com.baidu.kafka.elasticsearch;

public class ConfigFile {
  //Zookeeper
  public static String ZK_HOSTS = "10.170.130.133";
  public static int ZK_PORT = 2181;
  public static String ZK_STATE_UPDATE_INTERVAL_MS = "2000";
  public static String ZK_SESSION_TIMEOUT_MS = "500";
  public static String ZK_SYNC_TIME_MS = "200";
  public static String AUTO_COMMIT_INTERVAL_MS = "1000";
  public static String CONSUMER_TIMEOUT_MS = "-1";
  public static String AUTO_OFFSET_RESET = "largest";
  public static String REBALANCE_MS = "100";
  public static int THREAD_NUM = 3;
  public static int BUFFER_SIZE = 3000;
  //Elasticsearch本地虚拟机
  //public static String ES_HOSTS = "192.168.79.129";
  public static String ES_HOSTS = "10.170.130.154";
  //public static int ES_PORT = 8300;
  public static int ES_PORT = 9300;
  public static String ES_CLUSTER_NAME = "my-application";
  public static int INDEX_INTERVAL = 10;
  //Kafka
  public static String KAFKA_GROUPID = "kafka_estest_group";
  public static String KAFKA_TOPIC = "base_yt_app";
}
