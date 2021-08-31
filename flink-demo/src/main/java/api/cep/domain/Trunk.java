package api.cep.domain;

import jdk.nashorn.internal.objects.annotations.Constructor;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zdp
 * @description 流量
 * @email 13221018869@189.cn
 * @date 2021/6/28 11:32
 */
@Data
@AllArgsConstructor
public class Trunk{
    private String user;
    private long time;
    private long trunkValue;

    public Trunk(){}


    @Override
    public String toString() {
        return "Trunk{" +
                "user='" + user + '\'' +
                ", time=" + time +
                ", trunkValue=" + trunkValue +
                '}';
    }
}
