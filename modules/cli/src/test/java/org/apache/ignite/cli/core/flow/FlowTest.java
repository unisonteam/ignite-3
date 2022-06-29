package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private String s;

    @Test
    public void test() {
        FlowExecutionPipeline<StringCallInput, Integer> pipeline = FlowExecutionPipeline
                .<StringCallInput, String>builder(input -> DefaultFlowOutput.success(input.getString()))
                .inputProvider(() -> new StringCallInput(s))
                .appendFlow(input -> {
                    if (input.hasResult()) {
                        try {
                            return DefaultFlowOutput.success(Integer.parseInt(input.body()));
                        } catch (NumberFormatException e) {
                            return DefaultFlowOutput.failure(e);
                        }
                    }
                    throw new FlowInterruptException(input.errorCause());
                }, FlowInterrupter.identity())
                .appendFlow(input -> {
                    if (input.hasError()) {
                        return DefaultFlowOutput.success(-1);
                    }
                    return input;
                }, FlowInterrupter.build(data -> () -> String.valueOf(data)))
                .build();

        s = "5";
        pipeline.runPipeline();
        s = "s";
        pipeline.runPipeline();
    }
}
