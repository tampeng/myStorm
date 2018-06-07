package com.study.storm.storm_kafka;


import com.study.storm.utils.Constant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Storm2KafkaTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(Storm2KafkaTopology.class);

    public static void main(String[] args) {
        LOGGER.info("Storm2KafkaTopology拓扑开始启动");

        // BrokerHosts接口有2个实现类StaticHosts和ZkHosts,ZkHosts会定时(默认60秒)从ZK中更新brokers的信息,StaticHosts是则不会
        // 要注意这里的第二个参数brokerZkPath要和kafka中的server.properties中配置的zookeeper.connect对应
        // 因为这里是需要在zookeeper中找到brokers znode
        // 默认kafka的brokers znode是存储在zookeeper根目录下
        BrokerHosts brokerHosts = new ZkHosts(Constant.ZK_HOST_PORT);

        String topic = Constant.TOPIC_TEST;
        String zkROOT = "";
        String spoutId = Constant.SPOUT_TEST;

        // 定义spoutConfig
        // 第一个参数hosts是上面定义的brokerHosts
        // 第二个参数topic是该Spout订阅的topic名称
        // 第三个参数zkRoot是存储消费的offset(存储在ZK中了),当该topology故障重启后会将故障期间未消费的message继续消费而不会丢失(可配置)
        // 第四个参数id是当前spout的唯一标识
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,topic,"",spoutId);

/*        List<String> zkServers = new ArrayList<String>() ;
        zkServers.add("192.168.128.10");
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000 ;*/

        // 定义kafkaSpout如何解析数据,这里是将kafka的producer send的数据放入到String
        // 类型的str变量中输出,这个str是StringSchema定义的变量名称
        // 指定kafka消息为String
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        LOGGER.info("SpoutConfig设置完成");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout",new KafkaSpout(spoutConfig),1);
        builder.setBolt("SentenceBost",new SentenceBolt(),3).shuffleGrouping("KafkaSpout");
        builder.setBolt("PrinterBost",new PrinterBolt(),3).shuffleGrouping("SentenceBost");

        Config config = new Config();
        config.setDebug(false) ;
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStrom",config, builder.createTopology());

/*        // 本地运行或者提交到集群
        if (args != null && args.length == 1) {
            // 集群运行
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }else {
            // 本地运行
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("K2Stopology", config, builder.createTopology());
            // 这里为了测试方便就不shutdown了
            Thread.sleep(10000000);
            // cluster.shutdown();
        }*/

    }
}
