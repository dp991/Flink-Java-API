package api.clickhouse.utils;

import api.clickhouse.domain.TrunkFlowDto;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import table.Msg;

import javax.xml.crypto.Data;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @author zdp
 * @description 工具类
 * @email 13221018869@189.cn
 * @date 2021/6/30 10:40
 */
public class Helper {

    public static List<TrunkFlowDto> string2Dto(String s) {
        JSONObject msg = JSON.parseObject(s);
        List<TrunkFlowDto> list = new ArrayList<>();
        if (msg != null) {
            String nfid = msg.getString("nfid");
            String type = msg.getString("type");
            String time = mills2DateString(msg.getString("timetick"));

            JSONArray jsonArray = msg.getJSONArray("data");
            Iterator<Object> iterator = jsonArray.stream().iterator();
            while (iterator.hasNext()) {
                TrunkFlowDto dto = new TrunkFlowDto();
                dto.setNfid(nfid);
                dto.setType(type);
                dto.setTime(time);
                JSONObject object = JSON.parseObject(iterator.next().toString());
                dto.setPort(object.getString("port"));
                dto.setPort_speed(object.getString("port_speed"));
                dto.setFix_speed(object.getString("fix_speed"));
                dto.setRecv_pkt(object.getLong("recv_pkt"));
                dto.setRecv_byte(object.getLong("recv_byte"));
                dto.setRecv_drop_pkt(object.getLong("recv_drop_pkt"));
                dto.setRecv_err_pkt(object.getLong("recv_err_pkt"));
                dto.setRecv_pps(object.getLong("recv_pps"));
                dto.setRecv_bps(object.getLong("recv_bps"));
                dto.setSend_pkt(object.getLong("send_pkt"));
                dto.setSend_byte(object.getLong("send_byte"));
                dto.setSend_drop_pkt(object.getLong("send_drop_pkt"));
                dto.setSend_err_pkt(object.getLong("send_err_pkt"));
                dto.setSend_bps(object.getLong("send_bps"));
                dto.setSend_pps(object.getLong("send_bps"));

                list.add(dto);
            }
        }
        return list;
    }

    public static String mills2DateString(String mills) {
        String time = "1979-01-01 00:00:00";
        if (!StringUtils.isBlank(mills)) {
            //毫秒数
            long l = Long.parseLong(mills);
            //毫秒数转换成date
            Date date = new Date(l);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            time = sdf.format(date);
        }
        return time;
    }

    public static List<Msg> parseFromJSONString(String s) {
        JSONObject msg = JSON.parseObject(s);
        List<Msg> list = new ArrayList<>();

        if (msg != null) {
            String nfid = msg.getString("nfid");
            String type = msg.getString("type");
            String time = mills2DateString(msg.getString("timetick").trim());
            JSONObject account = msg.getJSONObject("account");
            String vni = account.getString("vni");
            String pvlan = account.getString("pvlan");
            String cvlan = account.getString("cvlan");
            JSONArray data = msg.getJSONArray("data");
            Iterator<Object> iterator = data.iterator();
            while (iterator.hasNext()) {
                Msg m1 = new Msg();
                JSONObject next = (JSONObject) iterator.next();
                m1.setNfid(nfid);
                m1.setCvlan(cvlan);
                m1.setPvlan(pvlan);
                m1.setType(type);
                m1.setTime(time);
                m1.setVni(vni);
                m1.setMac(next.getString("mac"));
                m1.setReceivePkt(next.getLongValue("recv_pkt"));
                m1.setReceivebyte(next.getLongValue("recv_byte"));
                m1.setReceiveDropPkt(next.getLongValue("recv_drop_pkt"));
                m1.setReceiveBps(next.getLongValue("recv_bps"));
                m1.setReceivePktAdd(next.getLongValue("recv_pkt_add"));
                m1.setReceivebyteAdd(next.getLongValue("recv_byte_add"));
                m1.setSendPkt(next.getLongValue("send_pkt"));
                m1.setSendByte(next.getLongValue("send_byte"));
                m1.setSendDropPkt(next.getLongValue("send_drop_pkt"));
                m1.setSendBps(next.getLongValue("send_bps"));
                m1.setSendPktAdd(next.getLongValue("send_pkt_add"));
                m1.setSendByteAdd(next.getLongValue("send_byte_add"));
                list.add(m1);
            }
        }
        return list;
    }
}
