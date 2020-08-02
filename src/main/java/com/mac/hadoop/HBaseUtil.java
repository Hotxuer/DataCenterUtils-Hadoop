package com.mac.hadoop;


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    /**
     * 创建表
     *
     * @param tableName 创建表的表名称
     * @param cfs       列簇的集合
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        TableName tn = TableName.valueOf(tableName);
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(tn)) {
                System.out.println("table already exists!");
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @return
     */
    public static boolean deleteTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (!admin.tableExists(tn)) {
                return false;
            }
            admin.disableTable(tn);
            admin.deleteTable(tn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param qualifer
     * @param data
     * @return
     */
    public static boolean putRow(String tableName, String rowkey, String cfName, String qualifer, String data) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifer), Bytes.toBytes(data));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 批量出入数据
     *
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try (Table table = HBaseConn.getTable(tableName)) {
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 查询单条数据
     *
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param qualifer
     * @return
     */
    public static Result getRow(String tableName, String rowkey, String cfName, String qualifer) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifer));
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}