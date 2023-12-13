package com.cdc.params;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetaData {

    public static Map<String, Map<String, List<String>>> tablesMetaData;

    public static void getMetaData(Properties properties) {
        String[] tables = BaseParameters.tables;
        tablesMetaData = Arrays.stream(tables)
                .collect(Collectors.toMap(Function.identity(), tableName -> getTableMetaData(properties, tableName)));
    }

    public static Map<String, List<String>> getTableMetaData(Properties properties, String tableName) {


        String url = properties.getProperty("url");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        String databaseName = properties.getProperty("databaseName");
        Map<String, List<String>> metadata = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        List<String> totalSize = new ArrayList<>(1);
        Integer size = 0;
        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            DatabaseMetaData dbMetadata = conn.getMetaData();

            ResultSet columns = dbMetadata.getColumns(conn.getCatalog(), databaseName, tableName, null);
            while (columns.next()) {
                columnNames.add(columns.getString("COLUMN_NAME") + " " + columns.getString("TYPE_NAME"));
                String columnType = columns.getString("TYPE_NAME");
                Integer columnSize = columns.getInt("COLUMN_SIZE");
                size += size(columnType, columnSize);
            }
            totalSize.add(size.toString());
            ResultSet primaryKeyResultSet = dbMetadata.getPrimaryKeys(conn.getCatalog(), databaseName, tableName);
            while (primaryKeyResultSet.next()) {
                primaryKeys.add(primaryKeyResultSet.getString("COLUMN_NAME"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        metadata.put("columns", columnNames);
        metadata.put("primaryKeys", primaryKeys);
        metadata.put("totalSize", totalSize);

        return metadata;
    }

    public static int size(String columnType, Integer columnSize) {
        switch (columnType) {
            case "INT":
            case "FLOAT":
                return 4;
            case "DECIMAL":
                return (int) Math.ceil((double) columnSize / 9);
            case "CHAR":
            case "VARCHAR":
                return columnSize;
            case "TIMESTAMP":
                return 8;
        }
        return 0;
    }

}
