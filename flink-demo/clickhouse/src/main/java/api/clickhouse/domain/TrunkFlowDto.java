package api.clickhouse.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zdp
 * @description trunk流量实体类
 * @email 13221018869@189.cn
 * @date 2021/6/30 9:08
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrunkFlowDto {
    private String nfid;
    private String type;
    private String port;
    private String port_speed;
    private String fix_speed;
    private long recv_pkt;
    private long recv_byte;
    private long recv_drop_pkt;
    private long recv_err_pkt;
    private long recv_pps;
    private long recv_bps;
    private long send_pkt;
    private long send_byte;
    private long send_drop_pkt;
    private long send_err_pkt;
    private long send_pps;
    private long send_bps;
    private String time;

    public static String convertToCsv(TrunkFlowDto trunkFlowDto) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        builder.append("'").append(trunkFlowDto.getNfid()).append("',");
        builder.append("'").append(trunkFlowDto.getType()).append("',");
        builder.append("'").append(trunkFlowDto.getPort()).append("',");
        builder.append("'").append(trunkFlowDto.getPort_speed()).append("',");
        builder.append("'").append(trunkFlowDto.getFix_speed()).append("',");

        // Java Bean 必须实现的方法，信息通过字符串进行拼接
        builder.append(trunkFlowDto.getRecv_pkt()).append(",").append(trunkFlowDto.getRecv_byte()).append(",").
                append(trunkFlowDto.getRecv_drop_pkt()).append(",")
                .append(trunkFlowDto.getRecv_err_pkt()).append(",").append(trunkFlowDto.getRecv_pps()).append(",")
                .append(trunkFlowDto.getRecv_bps()).append(",").append(trunkFlowDto.getSend_pkt()).append(",")
                .append(trunkFlowDto.getSend_byte()).append(",").append(trunkFlowDto.getSend_drop_pkt()).append(",")
                .append(trunkFlowDto.getSend_err_pkt()).append(",").append(trunkFlowDto.getSend_pps()).append(",")
                .append(trunkFlowDto.getSend_bps()).append(",");

        builder.append("'").append(trunkFlowDto.getTime()).append("'");
        builder.append(")");

        return builder.toString();
    }

    @Override
    public String toString() {
        return "TrunkFlowDto{" +
                "nfid='" + nfid + '\'' +
                ", type='" + type + '\'' +
                ", port='" + port + '\'' +
                ", port_speed='" + port_speed + '\'' +
                ", fix_speed='" + fix_speed + '\'' +
                ", recv_pkt=" + recv_pkt +
                ", recv_byte=" + recv_byte +
                ", recv_drop_pkt=" + recv_drop_pkt +
                ", recv_err_pkt=" + recv_err_pkt +
                ", recv_pps=" + recv_pps +
                ", recv_bps=" + recv_bps +
                ", send_pkt=" + send_pkt +
                ", send_byte=" + send_byte +
                ", send_drop_pkt=" + send_drop_pkt +
                ", send_err_pkt=" + send_err_pkt +
                ", send_pps=" + send_pps +
                ", send_bps=" + send_bps +
                ", time='" + time + '\'' +
                '}';
    }
}
