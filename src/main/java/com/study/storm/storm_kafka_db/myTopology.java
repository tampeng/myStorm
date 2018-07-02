package com.study.storm.storm_kafka_db;

import com.study.storm.utils.Constant;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class myTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(myTopology.class);
    public static void main(String args[]) throws InterruptedException {
        LOGGER.info("SpoutConfig设置...");
        BrokerHosts zhHost = new ZkHosts(Constant.ZK_HOST_PORT);
        String topic = Constant.TOPIC_TEST;
        String zkROOT = "";
        String spoutId = Constant.SPOUT_TEST;

        SpoutConfig spoutConfig = new SpoutConfig(zhHost,topic,zkROOT,spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConfig.forceFromStart = false;
        LOGGER.info("SpoutConfig设置完成");
        LOGGER.info("Topology设置...");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout",new KafkaSpout(spoutConfig),1);
        builder.setBolt("Splitter",new SplitterBolt(),3).shuffleGrouping("KafkaSpout");
        builder.setBolt("Couter",new CounterBolt(),3).fieldsGrouping("Splitter",new Fields("word"));
        LOGGER.info("Topology设置完成");
        Config config = new Config();

        if (args != null && args.length == 1){
            // 集群运行
            LOGGER.info("线上提交Topology");
            try {
                StormSubmitter.submitTopology(args[0],config ,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LOGGER.info("本地提交Topology");
            // 本地运行
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Kafka2StormTopology2db",config,builder.createTopology());
            // 这里为了测试方便就不shutdown了
            Thread.sleep(10000000);
            localCluster.shutdown();
        }
    }
}
