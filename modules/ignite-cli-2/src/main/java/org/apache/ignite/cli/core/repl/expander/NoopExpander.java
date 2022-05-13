package org.apache.ignite.cli.core.repl.expander;

import org.jline.reader.Expander;
import org.jline.reader.History;

public class NoopExpander implements Expander {
    @Override
    public String expandHistory(History history, String line) {
        return line;
    }

    @Override
    public String expandVar(String word) {
        return word;
    }
}
