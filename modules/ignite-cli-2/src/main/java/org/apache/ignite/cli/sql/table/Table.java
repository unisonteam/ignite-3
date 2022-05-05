package org.apache.ignite.cli.sql.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data class for table representation.
 *
 * @param <T> type of table elements.
 */
public class Table<T> {
    private final Map<String, TableRow<T>> content;

    /**
     * Constructor.
     *
     * @param ids     list of row ids.
     * @param content list of row content. Size should be equals n * ids.size.
     */
    public Table(List<String> ids, List<T> content) {
        if (content.size() % ids.size() != 0) {
            throw new RuntimeException();
        }

        this.content = new HashMap<>();
        for (int i = 0, size = ids.size(); i < size; i++) {
            String id = ids.get(i);
            TableRow<T> row = new TableRow<>(id, content.subList(i * size, (i + 1) * size));
            this.content.put(id, row);
        }
    }

    /**
     * Row getter.
     *
     * @param id row id.
     * @return Table row with {@param id}.
     */
    public TableRow<T> getRow(String id) {
        return content.get(id);
    }

    /**
     * Table header getter.
     *
     * @return array of table's columns name.
     */
    public String[] header() {
        return content.keySet().toArray(new String[0]);
    }

    /**
     * Table content getter.
     *
     * @return content of table without header.
     */
    public Object[][] content() {
        List<Object[]> collect = content.values().stream()
                .map(ts -> new ArrayList<>(ts.getValues()))
                .map(strings -> strings.toArray(new Object[0]))
                .collect(Collectors.toList());

        return collect.toArray(new Object[0][0]);
    }

    /**
     * Create method.
     *
     * @param resultSet coming result set.
     * @return istance of {@link Table}.
     */
    public static Table<String> fromResultSet(ResultSet resultSet) {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                ids.add(metaData.getColumnName(i));
            }
            List<String> content = new ArrayList<>();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    content.add(resultSet.getString(i));
                }
            }
            return new Table<>(ids, content);
        } catch (SQLException e) {
            return null;
        }
    }
}
