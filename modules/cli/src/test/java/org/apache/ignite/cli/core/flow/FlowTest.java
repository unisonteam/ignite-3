package org.apache.ignite.cli.core.flow;

import java.util.function.Predicate;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private String s;

    @Test
    public void test() {
        FlowExecutionPipeline<StringCallInput, ?> pipeline = FlowExecutionPipeline
                .<StringCallInput, String>builder()
                .inputProvider(() -> new StringCallInput(s))
                .appendFlow((input, interrupt) -> DefaultFlowOutput.success(input.body()))
                .appendFlow((input, interrupt) -> {
                        try {
                            return DefaultFlowOutput.success(Integer.parseInt(input.body()));
                        } catch (NumberFormatException e) {
                            interrupt.interrupt(input);
                            return null;
                        }
                })
                .branches()
                .branch(integer -> integer % 2 == 0, (input, interrupt) -> DefaultFlowOutput.success(input.body()))
                .branch(integer -> integer % 2 == 1, (input, interrupt) -> DefaultFlowOutput.success(input.body() + 1))
                .build();


        s = "5";
        pipeline.runPipeline();
        s = "s";
        pipeline.runPipeline();
    }
}
