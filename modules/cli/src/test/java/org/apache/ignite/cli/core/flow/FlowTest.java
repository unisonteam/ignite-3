package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.core.call.StringCallInput;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private String s;

    @Test
    public void test() {

        FlowExecutionPipeline<StringCallInput, Integer> pipeline = FlowExecutionPipeline.builder(StringCallInput::getString)
                .inputProvider(() -> new StringCallInput(s))
                .appendFlow(input -> {
                    try {
                        return Integer.parseInt(input);
                    } catch (NumberFormatException e) {
                        throw new FlowInterruptException();
                    }
                }, System.out::println).output(System.out).build();

        s = "5";
        pipeline.runPipeline();
        s = "s";
        pipeline.runPipeline();
    }
}
