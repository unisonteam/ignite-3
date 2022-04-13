package org.apache.ignite.cli.commands.sql;

import static org.junit.jupiter.api.Assertions.assertAll;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@MicronautTest
class ItSqlReplCommandTest extends IntegrationTestBase {

    @Inject
    SqlReplCommand sqlReplCommand;

    @BeforeEach
    void setUp() {
        createAndPopulateTable();
        setupCmd(sqlReplCommand);
    }

    @AfterEach
    void tearDown() {
        dropAllTables();
    }

    @Test
    @DisplayName("Should execute select * from table and display table when jdbc-url is correct")
    void selectFromTable() {
        execute("--execute", "select * from person", "--jdbc-url", JDBC_URL);

        assertAll(
                () -> assertExitCodeIs(0),
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should display readable error when wrong jdbc is given")
    void wrongJdbcUrl() {
        execute("--execute", "select * from person", "--jdbc-url", "jdbc:ignite:thin://no-such-host.com:10800");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                this::assertErrOutputIsNotEmpty,
                // todo: specify error output
                () -> assertErrOutputIs("Cannot connect to jdbc")
        );

    }
}