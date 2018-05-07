package com.study.storm.richStorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


/**
 * @Description: 将 from,to,duration 拼成 from + " - " + to, duration
 * @author: peng.tan
 * @time: 2018/5/7 19:38
 */
public class CallLogCreatorBolt implements IRichBolt {
    //收集数据，发送tuple
    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * 计算
     */
    public void execute(Tuple tuple) {
        String from = tuple.getString(0);
        String to = tuple.getString(1);
        Integer  duration = tuple.getInteger(2);
        collector.emit(new Values(from + " - " + to, duration));
    }

    public void cleanup() {

    }

    /**
     * 定义 field
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("call","duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
