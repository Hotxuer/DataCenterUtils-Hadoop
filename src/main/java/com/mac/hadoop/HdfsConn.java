package com.mac.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HdfsConn {
    private static Configuration conf = new Configuration();
    private static FileSystem hdfs;

    private HdfsConn() {

    }

    public static FileSystem getFileSys() {
        try {
            conf.set("fs.defaultFS", "hdfs://202.120.38.100:9000");
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            System.out.println("Hdfs connection error!");
            e.printStackTrace();
        }
        return hdfs;
    }
}
