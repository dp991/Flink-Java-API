package stream.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zdp
 * @description 时间转换类
 * @email 13221018869@189.cn
 * @date 2021/5/25 19:40
 */
public class DateUtil {

    /**
     * 毫秒数转成string
     *
     * @param timestamp 毫秒数
     * @return
     */
    public static String long2String(long timestamp) {

        Date date = new Date(timestamp);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        String s = format.format(date);
        return s;
    }
}
