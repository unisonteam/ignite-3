package org.apache.ignite.cli.core;

import jakarta.inject.Singleton;

@Singleton
public class RunningModeHolder {
    private Mode mode = Mode.NON_REPL;

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }
}
