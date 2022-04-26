package org.apache.ignite.cli;

import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;

public class TestCall<IT extends CallInput> implements Call<IT, String> {
    private IT input;

    @Override
    public CallOutput<String> execute(IT input) {
        this.input = input;
        return DefaultCallOutput.success("ok");
    }
}
