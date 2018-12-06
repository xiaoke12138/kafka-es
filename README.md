kafka2es使用
===================

编译命令：mvn -f pom.xml assembly:assembly

配置文件为.json文件，例如kafka2es-example.json:

    {
        "zkHost":"xxx", //zk的ip或者vip
        "esHost":"xxx",  //es集群的ip或者vip
        "esCluster":"es_test",    //es集群的名字
        "kafkaTopic":"xxx",   //订阅kafka的topic
        "kafkaGroup":"xxxx",  //订阅kafka的group
        "threadNum":"3",            //开启线程数
        "bulkMaxSize":"3000",       //批量创建索引的最大数据量
    }

运行命令：java -jar kafka2es kafka2es-example.json
