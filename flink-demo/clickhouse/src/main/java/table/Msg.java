package table;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Msg {

    private String nfid;
    private String type;

    private String vni;
    private String pvlan;
    private String cvlan;

    private String mac;
    private long receivePkt;
    private long receivebyte;
    private long receiveDropPkt;
    private long receiveBps;
    private long receivePktAdd;
    private long receivebyteAdd;

    private long sendPkt;
    private long sendByte;
    private long sendDropPkt;
    private long sendBps;
    private long sendPktAdd;
    private long sendByteAdd;

    private String time;
}
