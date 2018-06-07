package com.study.storm.richStorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @Description: 启动topology,在本地集群模式运行
 * @author: peng.tan
 * @time: 2018/5/7 20:07
 */
public class StormApp {
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        //设置spout
        builder.setSpout("call-spout",new CallSpout());
        //setBolt
        builder.setBolt("call-log-creator-bolt",new CallLogCreatorBolt()).shuffleGrouping("call-spout");
        //setBolt
        builder.setBolt("call-log-counter-bolt",new CallLogCounterBolt()).fieldsGrouping("call-log-creator-bolt",new Fields("call"));
        //本地集群
        LocalCluster cluster = new LocalCluster();

        //提交topology
        cluster.submitTopology("StormApp", config, builder.createTopology());
        Thread.sleep(300000);

        //停止集群
        cluster.shutdown();
    }
}
