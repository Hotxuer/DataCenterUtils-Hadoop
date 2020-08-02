package com.mac.hadoop;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;



import java.lang.String;
import java.util.List;

/**
 * Hello world!
 *
 */
public class videoApp
{
    private static HBaseUtil hBaseUtil = new HBaseUtil();
    private static HdfsUtil hdfsUtil = new HdfsUtil();
    private final static String tableName = "video";
    private final static String rootDir = "/video";

    public static void main(String[] args)
    {
//        initHbase();
//        uploadVideo("/Users/mac/Downloads/videoTest.txt", "videoTest", "2020-08-02", "linyi");
        downloadVideo("videoTest", "./");

    }

    /**
     * 后续需要添加对时间等信息的格式检查与修正
     */
    public static void uploadVideo(String srcPath, String videoName, String dateTime, String address) {
        String path = rootDir+"/"+dateTime+"/";
        boolean isHbaseDateSuccess = hBaseUtil.putRow(tableName, videoName, "info", "dataTime", dateTime);
        boolean isHbaseAddressSuccess = hBaseUtil.putRow(tableName, videoName, "info", "address", address);
        boolean isHbasePathSuccess = hBaseUtil.putRow(tableName, videoName, "storage", "path", path);
        boolean isHdfsSuccess = hdfsUtil.uploadFile(srcPath, path+"/"+videoName);
        if (isHbaseAddressSuccess && isHbaseDateSuccess && isHbasePathSuccess && isHdfsSuccess) {
            System.out.println("upload video succeed!");
        } else {
            System.out.println("upload video error!");
        }
    }

    /**
     * 后续需要添加多种查询方式
     * @param videoName
     */
    public static void downloadVideo(String videoName, String destPath) {
        Result result = hBaseUtil.getRow(tableName, videoName, "storage", "path");
        if (result != null) {
            List<Cell> cs = result.listCells();
            System.out.println("found"+cs.size()+"files");
            for (Cell cell: cs) {
                String path = Bytes.toString(CellUtil.cloneValue(cell));
                boolean isSuccess = hdfsUtil.downloadFile(path+"/"+videoName, destPath);
                if (isSuccess) {
                    System.out.println("download video succeed!");
                } else {
                    System.out.println("download video error!");
                }
            }
        } else {
            System.out.println("video not exists!");
        }
    }

    public static void initHbase() {
        boolean isSuccess = hBaseUtil.createTable(tableName, new java.lang.String[] {"info", "storage"});
        if (isSuccess) {
            System.out.println("Hbase init succeed!");
        } else {
            System.out.println("Hbase init error!");
        }
    }

}
