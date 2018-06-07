package com.study.storm.basebasic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class myTopology {
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);
        //config.setNumWorkers(2);
        TopologyBuilder builder = new TopologyBuilder();
        //spout
        SpoutDeclarer spoutDeclarer = builder.setSpout("mySpout",new mySpout());
        //spoutDeclarer.setNumTasks(1);
        //bolt
        BoltDeclarer boltDeclarer = builder.setBolt("myBolt",new myBolt()).shuffleGrouping("mySpout");
        //boltDeclarer.setNumTasks(2);
        //本地集群
        LocalCluster cluster = new LocalCluster();

        //提交topology
        cluster.submitTopology("myTopology",config,builder.createTopology());
        System.out.println("over!!!");
        Thread.sleep(300000);

        cluster.shutdown();
    }
}
