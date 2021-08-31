import javax.xml.crypto.Data;
import java.util.Date;

/**
 * @author zdp
 * @description sdkl
 * @email 13221018869@189.cn
 * @date 2021/6/28 16:05
 */
public class TimeUtils {
    public static void main(String[] args) {

        System.out.println(System.currentTimeMillis());
        Date date = new Date();

        System.out.println(date.getTime());
        System.out.println(date.getSeconds());

    }
}
