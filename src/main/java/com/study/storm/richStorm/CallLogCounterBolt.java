package com.study.storm.richStorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: 统计会话次数，时间
 * @author: peng.tan
 * @time: 2018/5/7 19:51
 */
public class CallLogCounterBolt implements IRichBolt{
    private static final long serialVersionUID = -4598302147870421925L;
    OutputCollector collector;
    Map<String,String> counterMap;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counterMap = new HashMap<String, String>();
    }

    public void execute(Tuple tuple) {
        String call = tuple.getString(0);
        Integer duration = tuple.getInteger(1);
        if (!counterMap.containsKey(call)){
            counterMap.put(call,"1," + duration.toString());
        } else {
            Integer c = new Integer(counterMap.get(call).split(",")[0]) + 1;
            Integer s = new Integer(counterMap.get(call).split(",")[1]) + duration;
            counterMap.put(call,c.toString() +","+ s.toString());
        }
        collector.ack(tuple);
    }

    public void cleanup() {
        for (Map.Entry<String,String> entry : counterMap.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
