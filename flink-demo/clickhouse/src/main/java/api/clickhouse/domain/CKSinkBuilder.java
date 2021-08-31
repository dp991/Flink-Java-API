package api.clickhouse.domain;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CKSinkBuilder implements JdbcStatementBuilder<TrunkFlowDto> {

    @Override
    public void accept(PreparedStatement preparedStatement, TrunkFlowDto dto) throws SQLException {

        preparedStatement.setString(1,dto.getNfid());
        preparedStatement.setString(2,dto.getType());
        preparedStatement.setString(3,dto.getPort());
        preparedStatement.setString(4,dto.getPort_speed());
        preparedStatement.setString(5,dto.getFix_speed());
        preparedStatement.setLong(6,dto.getRecv_pkt());
        preparedStatement.setLong(7,dto.getRecv_byte());
        preparedStatement.setLong(8,dto.getRecv_drop_pkt());
        preparedStatement.setLong(9,dto.getRecv_err_pkt());
        preparedStatement.setLong(10,dto.getRecv_pps());
        preparedStatement.setLong(11,dto.getRecv_bps());
        preparedStatement.setLong(12,dto.getSend_pkt());
        preparedStatement.setLong(13,dto.getSend_byte());
        preparedStatement.setLong(14,dto.getRecv_drop_pkt());
        preparedStatement.setLong(15,dto.getSend_err_pkt());
        preparedStatement.setLong(16,dto.getSend_pps());
        preparedStatement.setLong(17,dto.getSend_bps());
        preparedStatement.setString(18,dto.getTime());


    }
}
