package org.apache.ignite.cli.commands.decorators.core;

public interface Decorator<CommandDateType, TerminalDataType extends TerminalOutput> {
    TerminalDataType decorate(CommandDateType date);
}
