package com.study.storm.storm_kafka;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SentenceBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -3286331372283305392L;

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseBasicBolt.class);
    private List<String> words = new ArrayList<String>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);
        if (StringUtils.isBlank(word))
            return;
        LOGGER.info(word);
        words.add(word);
        if (word.endsWith(".")){
            basicOutputCollector.emit(ImmutableList.of((Object) StringUtils.join(words,' ')));
            words.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
