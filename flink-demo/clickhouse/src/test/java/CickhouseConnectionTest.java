import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author zdp
 * @description xs
 * @email 13221018869@189.cn
 * @date 2021/7/1 19:38
 */
public class CickhouseConnectionTest {
    public static void main(String[] args) throws Exception{

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection aDefault = DriverManager.getConnection("jdbc:clickhouse://10.50.51.46:8123/default", "default", "123456");
        System.out.println(aDefault);

    }
}
