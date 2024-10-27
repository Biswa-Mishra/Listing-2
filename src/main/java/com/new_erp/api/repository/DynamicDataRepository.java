package com.new_erp.api.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class DynamicDataRepository {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataRepository.class);
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DynamicDataRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> fetchData(String schema, String table, Map<String, Object> filters,
                                               String dateColumn, String fromDate, String toDate,
                                               String sortBy, String sortOrder) {
        validateSchemaAndTable(schema, table);

        StringBuilder sql = new StringBuilder("SELECT * FROM " + schema + "." + table + " WHERE 1=1");
        Map<String, Object> queryParams = new HashMap<>();

        if (dateColumn != null && fromDate != null) {
            sql.append(" AND ").append(dateColumn).append(" > :fromDate");
            queryParams.put("fromDate", parseValue(fromDate, "timestamp"));
        }
        if (dateColumn != null && toDate != null) {
            sql.append(" AND ").append(dateColumn).append(" < :toDate");
            queryParams.put("toDate", parseValue(toDate, "timestamp"));
        }

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            validateFilterKey(schema, table, key);
            String columnType = getColumnType(schema, table, key);
            String operator = "=";
            if ("text".equals(columnType) || "varchar".equals(columnType)) {
                if (value instanceof String && ((String) value).contains("%")) {
                    operator = "LIKE";
                }
            } else if (value instanceof String && (((String) value).startsWith(">") || ((String) value).startsWith("<"))) {
                operator = ((String) value).substring(0, 1);
                value = ((String) value).substring(1).trim();
            }
            sql.append(" AND ").append(key).append(" ").append(operator).append(" :").append(key);
            queryParams.put(key, parseValue(value, columnType));
        }

        if (sortBy != null && isValidFilterColumn(schema, table, sortBy)) {
            sql.append(" ORDER BY ").append(sortBy);
            if ("desc".equalsIgnoreCase(sortOrder)) {
                sql.append(" DESC");
            } else {
                sql.append(" ASC");
            }
        }

        log.debug("Executing SQL: {}", sql);
        log.debug("With parameters: {}", queryParams);

        try {
            return jdbcTemplate.queryForList(sql.toString(), queryParams);
        } catch (Exception e) {
            log.error("Error executing query: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to fetch data from " + table + " in " + schema, e);
        }
    }

    public boolean isValidFilterColumn(String schema, String table, String column) {
        String sql = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = :schema " +
                "AND table_name = :table AND column_name = :column)";
        Map<String, Object> params = Map.of("schema", schema, "table", table, "column", column);
        Boolean exists = jdbcTemplate.queryForObject(sql, params, Boolean.class);
        return Boolean.TRUE.equals(exists);
    }

    private String getColumnType(String schema, String table, String columnName) {
        String sql = "SELECT data_type FROM information_schema.columns WHERE table_schema = :schema " +
                "AND table_name = :table AND column_name = :column";
        Map<String, Object> params = Map.of("schema", schema, "table", table, "column", columnName);
        return jdbcTemplate.queryForObject(sql, params, String.class);
    }

    private Object parseValue(Object value, String columnType) {
        try {
            switch (columnType) {
                case "integer":
                    return Integer.parseInt(value.toString());
                case "float":
                    return Float.parseFloat(value.toString());
                case "boolean":
                    return Boolean.parseBoolean(value.toString());
                case "date":
                    return Date.valueOf(value.toString());
                case "timestamp without time zone":
                case "timestamp with time zone":
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    return new Timestamp(format.parse(value.toString()).getTime());
                default:
                    return value;
            }
        } catch (ParseException e) {
            log.error("Failed to parse value: {} for type: {}", value, columnType);
            throw new IllegalArgumentException("Invalid format for value: " + value + " expected type: " + columnType);
        }
    }

    private void validateSchemaAndTable(String schema, String table) {
        String sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table)";
        Map<String, Object> params = Map.of("schema", schema, "table", table);
        Boolean exists = jdbcTemplate.queryForObject(sql, params, Boolean.class);
        if (Boolean.FALSE.equals(exists)) {
            throw new IllegalArgumentException("Invalid schema or table name: " + schema + "." + table);
        }
    }

    private void validateFilterKey(String schema, String table, String columnName) {
        String sql = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = :schema " +
                "AND table_name = :table AND column_name = :column)";
        Map<String, Object> params = Map.of("schema", schema, "table", table, "column", columnName);
        Boolean exists = jdbcTemplate.queryForObject(sql, params, Boolean.class);
        if (Boolean.FALSE.equals(exists)) {
            throw new IllegalArgumentException("Invalid filter column: " + columnName + " in " + schema + "." + table);
        }
    }
}
