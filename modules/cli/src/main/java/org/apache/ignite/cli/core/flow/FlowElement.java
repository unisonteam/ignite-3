package org.apache.ignite.cli.core.flow;

import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.call.CallOutput;

public interface FlowElement<IT, OT> {
    FlowOutput<OT> call(IT input, FlowInterrupter interrupt);

    default <O> FlowElement<IT, O> composite(FlowElement<FlowOutput<OT>, O> next) {
        return (input, interrupt) -> {
            FlowOutput<OT> output = FlowElement.this.call(input, interrupt);
            return next.call(output, interrupt);
        };
    }

    static <I extends CallInput, T> FlowElement<I, T> fromCall(Call<I, T> call) {
        return (input, interrupt) -> {
            CallOutput<T> execute = call.execute(input);
            return DefaultFlowOutput
                    .<T>builder()
                    .body(execute.body())
                    .cause(execute.errorCause())
                    .build();
        };
    }
}
