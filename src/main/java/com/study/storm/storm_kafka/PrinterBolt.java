package com.study.storm.storm_kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -3112055859102440951L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String sentenct  = tuple.getString(0);
        System.out.println("Received Sentence:" + sentenct);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}