package table;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CKSinkBuilder implements JdbcStatementBuilder<Msg> {

    @Override
    public void accept(PreparedStatement preparedStatement, Msg msg) throws SQLException {

        preparedStatement.setString(1,msg.getNfid());
        preparedStatement.setString(2,msg.getType());
        preparedStatement.setString(3,msg.getVni());
        preparedStatement.setString(4,msg.getPvlan());
        preparedStatement.setString(5,msg.getCvlan());
        preparedStatement.setString(6,msg.getMac());
        preparedStatement.setLong(7,msg.getReceivePkt());
        preparedStatement.setLong(8,msg.getReceivebyte());
        preparedStatement.setLong(9,msg.getReceiveDropPkt());
        preparedStatement.setLong(10,msg.getReceiveBps());
        preparedStatement.setLong(11,msg.getReceivePktAdd());
        preparedStatement.setLong(12,msg.getReceivebyteAdd());
        preparedStatement.setLong(13,msg.getSendPkt());
        preparedStatement.setLong(14,msg.getSendByte());
        preparedStatement.setLong(15,msg.getSendDropPkt());
        preparedStatement.setLong(16,msg.getSendBps());
        preparedStatement.setLong(17,msg.getSendPktAdd());
        preparedStatement.setLong(18,msg.getSendByteAdd());
        preparedStatement.setString(19,msg.getTime());


    }
}
