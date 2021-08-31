package api.sink.hbaseSink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtil {

    /**
     * @param zkQuorum zookeeper地址，多个要用逗号分隔
     * @param port     zookeeper端口号
     * @return connection
     */
    public static Connection getConnection(String zkQuorum, int port) throws Exception {


        System.setProperty("hadoop.home.dir","D:\\config\\hadoop-2.7.4");
        System. setProperty("java.security.krb5.conf", "D:\\config\\kerberos\\krb5.conf" );


        Configuration conf = HBaseConfiguration.create();
        conf.addResource("D:\\work\\projects\\flink-demo\\src\\main\\resources\\hbase-site.xml");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("keytab.file" , "D:\\config\\kerberos\\hbase.keytab" );
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM");
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", port + "");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@HADOOP.COM");
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

//    /**
//     * test
//     */
    public static void main(String[] args) throws Exception {
        Connection connection = getConnection("hadoop-server-001,hadoop-server-002,hadoop-server-003", 2181);

        System.out.println("=====================连接成功=================="+connection.toString());
    }

}
