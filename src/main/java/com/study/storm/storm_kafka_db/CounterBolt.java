package com.study.storm.storm_kafka_db;

import com.study.storm.utils.Constant;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class CounterBolt extends BaseBasicBolt{
    private static final long serialVersionUID = 3147320678343461215L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseBasicBolt.class);

    Map<String,Integer>  result = new HashMap<>();
    private Connection conn;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        try {
            Class.forName(Constant.MYSQL_DRIVER);
            String url = Constant.MYSQL_URL;
            String user = Constant.MYSQL_USER;
            String password = Constant.MYSQL_PASSWORD;
            conn = DriverManager.getConnection(url,user,password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField("word");
        LOGGER.info(word);
        updateWordCount(word);
        }

        private void updateWordCount(String word){
            try {
                //查询
                PreparedStatement ppst = conn.prepareStatement("select num from words where word = ?");
                ppst.setString(1,word);
                ResultSet rs = ppst.executeQuery();
                int num = 0;
                if (rs.next()){
                    num = rs.getInt("num");
                }
                rs.close();
                ppst.close();
                String sql;
                //插入
                if (num == 0){
                    sql = "insert into words(word,num) values(?,1)";
                }//更新
                else {
                    sql = "update words set num = num + 1 where word = ?";
                }
                ppst = conn.prepareStatement(sql);
                ppst.setString(1,word);
                ppst.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
