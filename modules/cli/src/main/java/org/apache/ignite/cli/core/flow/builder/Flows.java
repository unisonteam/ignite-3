/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.flow.builder;

import java.util.List;
import java.util.function.Function;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.flow.DefaultFlowable;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionFactory;

public class Flows {

    public static <I extends CallInput, T> Flow<I, T> fromCall(Call<I, T> call) {
        return flowable -> fromOutput(call.execute(flowable.value()));
    }

    public static <T> FlowBuilder<Void, T> from(T value) {
        return new FlowBuilderImpl<>(mono(unused -> value));
    }

    public static <I, O> FlowBuilder<I, O> from(Function<I, O> function) {
        return new FlowBuilderImpl<>(mono(function));
    }

    public static <I, O> Flow<I, O> mono(Function<I, O> function) {
        return input -> Flowable.process(() -> function.apply(input.value()));
    }

    private static <T> Flowable<T> fromOutput(CallOutput<T> output) {
        return DefaultFlowable
                .<T>builder()
                .body(output.body())
                .cause(output.errorCause())
                .build();
    }

    public static <I, O> FlowBuilder<I, O> question(String question, List<QuestionAnswer<O>> answers) {
        return new FlowBuilderImpl<>(input -> Flowable.success(QuestionFactory.newQuestionAsker().askQuestion(question, answers)));
    }

}
