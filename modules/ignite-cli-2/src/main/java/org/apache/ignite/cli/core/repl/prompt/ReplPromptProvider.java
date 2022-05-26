package org.apache.ignite.cli.core.repl.prompt;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.repl.Session;

@Singleton
public class ReplPromptProvider implements PromptProvider {
    private final Session session;

    public ReplPromptProvider(Session session) {
        this.session = session;
    }

    @Override
    public String getPrompt() {
        return session.isConnectedToNode() ?
                "[" + session.getNodeUrl() + "]> " :
                "[disconnected]> ";
    }
}
