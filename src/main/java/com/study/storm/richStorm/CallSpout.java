package com.study.storm.richStorm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Description:  产生通话日志
 * @author: peng.tan
 * @time: 2018/5/7 19:10
 */
public class CallSpout implements IRichSpout {
    //创建tuple给bolt
    private SpoutOutputCollector collector;
    private boolean completed = false;

    //创建top上下文对象，含有top的数据
    private TopologyContext context;

    // Create instance for Random class.
    private Random rand = new Random();
    private Integer idx = 0;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if (idx <= 1000) {
            List<String> mobileNumbers = new ArrayList<String>();
            mobileNumbers.add("1234123401");
            mobileNumbers.add("1234123402");
            mobileNumbers.add("1234123403");
            mobileNumbers.add("1234123404");
            Integer localIdx = 0;
            while(localIdx++ < 100 && this.idx++ < 1000 ){
                //随机提取caller
                String fromNum = mobileNumbers.get(rand.nextInt(4));
                //随机提取receiver
                String toNum = mobileNumbers.get(rand.nextInt(4));
                while(fromNum == toNum) {
                    toNum = mobileNumbers.get(rand.nextInt(4));
                }
                //通话时长
                Integer duration = rand.nextInt(60);
                //输出tuple
                this.collector.emit(new Values(fromNum,toNum,duration));
            }
        }
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from","to","duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
