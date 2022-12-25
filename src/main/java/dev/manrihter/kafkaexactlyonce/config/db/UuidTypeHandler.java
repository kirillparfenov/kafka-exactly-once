package dev.manrihter.kafkaexactlyonce.config.db;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedTypes;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

@MappedTypes(UUID.class)
public class UuidTypeHandler extends BaseTypeHandler<UUID> {

    @Override
    public void setNonNullParameter(PreparedStatement preparedStatement, int i, UUID uuid, JdbcType jdbcType) throws SQLException {
        preparedStatement.setObject(i, uuid);
    }

    @Override
    public UUID getNullableResult(ResultSet resultSet, String s) throws SQLException {
        String value = resultSet.getString(s);
        if (value != null) {
            return UUID.fromString(value);
        }
        return null;
    }

    @Override
    public UUID getNullableResult(ResultSet resultSet, int i) throws SQLException {
        String value = resultSet.getString(i);
        if (value != null) {
            return UUID.fromString(value);
        }
        return null;
    }

    @Override
    public UUID getNullableResult(CallableStatement callableStatement, int i) throws SQLException {
        String value = callableStatement.getString(i);
        if (value != null) {
            return UUID.fromString(value);
        }
        return null;
    }
}
