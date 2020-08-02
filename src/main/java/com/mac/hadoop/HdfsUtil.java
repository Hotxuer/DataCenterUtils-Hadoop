package com.mac.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {
    public static boolean makeDir(String path) {
        try (FileSystem hdfs = HdfsConn.getFileSys()) {
            hdfs.mkdirs(new Path(path));
            System.out.println("make dictionary succeed!");
        } catch (Exception e) {
            System.out.println("make dictionary error!");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean uploadFile (String src, String dest) {
        try (FileSystem hdfs = HdfsConn.getFileSys()) {
            hdfs.copyFromLocalFile(new Path(src), new Path(dest));
            System.out.println("upload file succeed!");
        } catch (Exception e) {
            System.out.println("upload file error!");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean downloadFile (String src, String dest) {
        try (FileSystem hdfs = HdfsConn.getFileSys()) {
            hdfs.copyToLocalFile(new Path(src), new Path(dest));
            System.out.println("download file succeed!");
        } catch (Exception e) {
            System.out.println("download file error!");
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
