package org.apache.ignite.cli.sql.table;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TableTest {

    @Test
    void header() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        assertThat(table.header()).isEqualTo(new Object[] {"foo", "bar"});
    }

    @Test
    void content() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        assertThat(table.content()).isEqualTo(new Object[][]{new Object[]{"1", "2"}});
    }
}