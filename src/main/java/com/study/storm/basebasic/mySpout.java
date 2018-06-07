package com.study.storm.basebasic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class mySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private static int count = 0 ;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        List<String> list = new ArrayList<>();
        list.add("baidu");
        list.add("163");
        list.add("neteasy");
        list.add("beibei");
        list.add("google");


        Random rand = new Random();
        while (count <= 5){
            count++;
            collector.emit(new Values(list.get(rand.nextInt(5))));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("site"));
    }
}
