package test;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TSinkCKBuilder implements JdbcStatementBuilder<T> {

    @Override
    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {


        preparedStatement.setInt(1,t.getId());
        preparedStatement.setString(2,t.getName());
        preparedStatement.setString(3,t.getTime());
    }
}
