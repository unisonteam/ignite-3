package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.flow.builder.FlowExecutionPipeline;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private String s;
    private int result;
    private boolean interruptedParse;
    private boolean interruptedNullCheck;

    @Test
    public void test() {
        FlowExecutionPipeline<StringCallInput, ?> pipeline = FlowExecutionPipeline
                .<StringCallInput, String>builder((input, interrupt) -> DefaultFlowOutput.success(input.getString()))
                .inputProvider(() -> new StringCallInput(s))
                .appendFlow(this::parseInt)
                .interruptHandler(value -> interruptedParse = true)
                .appendFlow(this::interruptIfNull)
                .interruptHandler(value -> interruptedNullCheck = true)
                .branches()
                .branch(integer -> integer % 2 == 0, (input, interrupt) -> DefaultFlowOutput.success(result = input.body()))
                .branch(integer -> integer % 2 == 1, (input, interrupt) -> DefaultFlowOutput.success(result = input.body() + 1))
                .build();


        s = "5";
        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        pipeline.runPipeline();
        Assertions.assertFalse(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);
        Assertions.assertEquals(6, result);


        s = "4";
        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        pipeline.runPipeline();
        Assertions.assertEquals(4, result);
        Assertions.assertFalse(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);

        s = "s";
        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        pipeline.runPipeline();
        Assertions.assertEquals(-1, result);
        Assertions.assertTrue(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);

        s = "0";
        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        pipeline.runPipeline();
        Assertions.assertEquals(-1, result);
        Assertions.assertFalse(interruptedParse);
        Assertions.assertTrue(interruptedNullCheck);
    }

    @NotNull
    private FlowOutput<Integer> interruptIfNull(FlowOutput<Integer> input, FlowInterrupter interrupt) {
        if (input.body() == 0) {
            interrupt.interrupt(input);
        }
        return input;
    }

    @Nullable
    private DefaultFlowOutput<Integer> parseInt(FlowOutput<String> input, FlowInterrupter interrupt) {
        try {
            return DefaultFlowOutput.success(Integer.parseInt(input.body()));
        } catch (NumberFormatException e) {
            interrupt.interrupt(input);
            return null;
        }
    }
}
