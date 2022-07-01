package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.call.configuration.JsonString;
import org.apache.ignite.cli.call.status.Status;
import org.apache.ignite.cli.commands.decorators.ConfigDecorator;
import org.apache.ignite.cli.commands.decorators.JsonDecorator;
import org.apache.ignite.cli.commands.decorators.SqlQueryResultDecorator;
import org.apache.ignite.cli.commands.decorators.StatusDecorator;
import org.apache.ignite.cli.commands.decorators.TableDecorator;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.DecoratorRegistry;
import org.apache.ignite.cli.sql.SqlQueryResult;
import org.apache.ignite.cli.sql.table.Table;

public class DefaultDecoratorRegistry extends DecoratorRegistry {
    public DefaultDecoratorRegistry() {
        add(JsonString.class, new JsonDecorator());
        add(Config.class, new ConfigDecorator());
        add(Table.class, new TableDecorator());
        add(SqlQueryResult.class, new SqlQueryResultDecorator());
        add(Status.class, new StatusDecorator());

    }
}
