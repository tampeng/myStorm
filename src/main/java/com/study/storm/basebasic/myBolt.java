package com.study.storm.basebasic;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class myBolt implements IRichBolt {
    private static final long serialVersionUID = -4598302147870421925L;
    private OutputCollector collector;
    Map<String,Integer> content;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.content = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String site = tuple.getString(0);
        if (!content.containsKey(site)){
            content.put(site,1);
        } else {
            content.put(site,content.get(site) + 1);
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        for (Map.Entry<String,Integer> entry : content.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue() + " #" + Thread.currentThread().getName());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
