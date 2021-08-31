//package api.sink.hbaseSink;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//
//public class HbaseRichMapFunction extends RichSinkFunction<Tuple2<String,String>> {
//
//    private Connection conn = null;
//    private Table table1;
//
//    /**
//     *初始化hbase的连接
//     *open方法中可以使用ParameterTool工具传递参数
//     */
//    public void open(Configuration parameters) {
//        Configuration configuration = HbaseConfiguration.create();
//        configuration.addResource(new Path("/tmp/hbase-site.xml"));
//        conn = ConnectionFactory.createConnection(configuration);
//        table1 = conn.getTable(TableName.valueOf("myHbaseTableName"));
//    }
//
//    public String map(Tuple2<Boolean,String> ff){
//        String myRowkey = ff.f1;
//        Get myget = new Get(Bytes.toBytes(myRowkey));
//        Result re = table1.get(myget);
//        String custNum = Bytes.toString(re.getValue(Bytes.toBytes("f"),Bytes.toBytes("CUST_NUM")))
//        return custNum;
//    }
//
//    public void clost()throws Exception{
//        super.close();
//        conn.close();
//        table1.close();
//    }
//
//}
