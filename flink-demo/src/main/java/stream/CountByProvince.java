package stream;

/**
 * @author: create by zdp
 * @version: v1.0
 * @description: domain
 * @date:2020-02-25
 * 数据输出类
 */
public class CountByProvince {

    private String windowEnd;
    private String windowStart;
    private String province;
    private Long count;

    public CountByProvince(String windowStart,String windowEnd, String province, Long count) {
        this.windowStart=windowStart;
        this.windowEnd = windowEnd;
        this.province = province;
        this.count = count;
    }

    public CountByProvince(){}

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "("+windowStart+","+windowEnd+","+province+","+count+")";
    }
}
