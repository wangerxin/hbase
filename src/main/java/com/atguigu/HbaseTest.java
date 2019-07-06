package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseTest {

    private static Configuration configuration = null;
    private static Admin admin = null;


    public static void main(String[] args) throws IOException {

        // see github
        //1.配置信息
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //2.获取客户端对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        //1.判断表是否存在
        //isTbaleExist("student");

        //2.窗机表
//        createTable("emp2", "info1", "info2");

        //3.插入成功
        insert("emp","1000","info1","name","zhangsan");
        //关闭资源
        admin.close();
        connection.close();

    }

    //判断表是否存在
    public static boolean isTbaleExist(String tableName) throws IOException {

        //2.判断表是否存在
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        if (result) {
            System.out.println(tableName + "已存在");
        } else {
            System.out.println(tableName + "不存在");
        }
        return result;
    }

    //创建表
    public static void createTable(String tableName, String... cf) throws IOException {

        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "已经存在!");
            return;
        } else {
            //表描述器
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String s : cf) {
                //列表述器
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(s);
                tableDescriptor.addFamily(columnDescriptor);
            }
            admin.createTable(tableDescriptor);
            System.out.println(tableName + "创建成功");
        }
    }

    //删除表
    public static void dropTable(String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName + "已经删除");
        } else {
            System.out.println(tableName + "不存在");
        }
    }

    //插入数据
    public static void insert(String tableName, String rowkey,String cf, String c, String value) throws IOException {

        //1.创建客户端
        HTable hTable = new HTable(configuration, tableName);
        //2.封装put
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(c),Bytes.toBytes(value));
        //3.提交数据
        hTable.put(put);
        //4.关闭资源
        hTable.getClass();
        System.out.println("插入成功");
    }
}
