package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.flow.builder.Flow;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlowTest {

    private int result;
    private boolean parseException;

    @Test
    public void test() {


        Flow<String, Integer> flow = Flows.from(this::parseInt)
                .appendFlow(this::interruptIfNull)
                .ifThen(integer -> integer % 2 == 0, Flows.mono((Integer i) -> result = i))
                .ifThen(integer -> integer % 2 == 1, Flows.mono((Integer i) -> result = i + 1))
                .exceptionHandler(new ParseExceptionHandler())
                .build();

        result = -1;
        parseException = false;
        flow.call(Flowable.success("5"));
        Assertions.assertFalse(parseException);
        Assertions.assertEquals(6, result);


        result = -1;
        parseException = false;
        flow.call(Flowable.success("4"));
        Assertions.assertEquals(4, result);
        Assertions.assertFalse(parseException);

        result = -1;
        parseException = false;
        flow.call(Flowable.success("s"));
        Assertions.assertEquals(-1, result);
        Assertions.assertTrue(parseException);

        result = -1;
        parseException = false;
        flow.call(Flowable.success("0"));
        Assertions.assertEquals(-1, result);
        Assertions.assertFalse(parseException);
    }

    @NotNull
    private Flowable<Integer> interruptIfNull(Flowable<Integer> flowable) {
        if (flowable.hasResult() && flowable.value() == 0) {
            return Flowable.interrupt(flowable.value());
        }
        return flowable;
    }

    @Nullable
    private Integer parseInt(String string) {
        return Integer.parseInt(string);
    }

    class ParseExceptionHandler implements ExceptionHandler<NumberFormatException> {

        @Override
        public int handle(ExceptionWriter err, NumberFormatException e) {
            parseException = true;
            return 0;
        }

        @Override
        public Class<NumberFormatException> applicableException() {
            return NumberFormatException.class;
        }
    }
}
