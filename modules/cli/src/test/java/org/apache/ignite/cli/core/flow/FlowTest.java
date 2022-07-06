package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.core.flow.builder.Flow;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private int result;
    private boolean interruptedParse;
    private boolean interruptedNullCheck;

    @Test
    public void test() {


        Flow<String, Integer> flow = Flows.from(this::parseInt)
                .appendFlow(this::interruptIfNull)
                .ifThen(integer -> integer % 2 == 0, Flows.mono((Integer i) -> i))
                .ifThen(integer -> integer % 2 == 1, Flows.mono((Integer i) -> i))
                .build();

        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        flow.call(Flowable.success("5"));
        Assertions.assertFalse(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);
        Assertions.assertEquals(6, result);


        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        flow.call(Flowable.success("4"));
        Assertions.assertEquals(4, result);
        Assertions.assertFalse(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);

        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        flow.call(Flowable.success("s"));
        Assertions.assertEquals(-1, result);
        Assertions.assertTrue(interruptedParse);
        Assertions.assertFalse(interruptedNullCheck);

        result = -1;
        interruptedParse = false;
        interruptedNullCheck = false;
        flow.call(Flowable.success("0"));
        Assertions.assertEquals(-1, result);
        Assertions.assertFalse(interruptedParse);
        Assertions.assertTrue(interruptedNullCheck);
    }

    @NotNull
    private Flowable<Integer> interruptIfNull(Flowable<Integer> flowable) {
        if (flowable.value() == 0) {
            return Flowable.interrupt(flowable.value());
        }
        return flowable;
    }

    @Nullable
    private Integer parseInt(String string) {
        return Integer.parseInt(string);
    }
}
