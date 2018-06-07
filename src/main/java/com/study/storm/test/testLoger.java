package com.study.storm.test;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testLoger {
    private static final Logger LOGGER = LoggerFactory.getLogger(testLoger.class);
    //LOGGER.info("begin0");

    public static void main(String[] args) {
    LOGGER.info("begin");
    System.out.println("hello LOGGER");
    LOGGER.debug("debug");
    }
}
