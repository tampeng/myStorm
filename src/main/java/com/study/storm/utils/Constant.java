package com.study.storm.utils;

public class Constant {
    public static final String ZK_HOST_PORT   = "192.168.128.10:2181,192.168.128.11:2181,192.168.128.12:2181";
    public static final String ZK_HOST_PORT_LOCAL   = "192.168.1.102:2181";
    public static final String ZK_HOST_PATH   = "kafka_new";
    public static final String ZK_ROOT        = "/rt_trade";

    public static final String TOPIC_TEST	= "test";

    public static final String SPOUT_TEST = "kafka2storm";

    // mysql
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://192.168.128.11:3306/test";
    public static final String MYSQL_USER = "bigdata";
    public static final String MYSQL_PASSWORD = "123456";
}
