package api.sink.hbaseSink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.List;

public class MyHbaseSink extends RichSinkFunction<Tuple2<String, String>> {

    Integer maxSize = 10;

    public MyHbaseSink() {
    }

    public MyHbaseSink(Integer maxSize) {
        this.maxSize = maxSize;
    }

      Connection connection;

      List<Put> puts = new ArrayList<>(maxSize);

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建连接
        super.open(parameters);

        //创建一个Hbase的连接
        connection = HBaseUtil.getConnection("hadoop-server-001,hadoop-server-002,hadoop-server-003",2181);

    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {

        String tableName = "student";
        //rowkey
        String rowKey = value.f0;
        //创建put对象，并赋rowKey值
        Put put = new Put(rowKey.getBytes());

        // 添加值：f1->列族, order->属性名 如age， 第三个->属性值 如25
        System.out.println("======================="+value.f0+"=============="+value.f1);
        put.addColumn("cf1".getBytes(), "name".getBytes(), value.f1.getBytes());
        puts.add(put);


        //开始批次提交数据
        if (puts.size() == maxSize) {

            //获取一个Hbase表
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(puts);//批次提交
            puts.clear();
            table.close();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null){
            connection.close();
        }
    }
}
