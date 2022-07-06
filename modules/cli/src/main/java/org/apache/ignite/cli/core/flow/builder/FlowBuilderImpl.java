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

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;

public class FlowBuilderImpl<I, O> implements FlowBuilder<I, O> {

    private final Flow<I, O> flow;
    private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();
    private PrintWriter output;
    private PrintWriter errorOutput;

    FlowBuilderImpl(Flow<I, O> flow) {
        this(flow, null, null, new DefaultExceptionHandlers());
    }

    FlowBuilderImpl(Flow<I, O> flow, PrintWriter output, PrintWriter errorOutput, ExceptionHandlers exceptionHandlers) {
        this.flow = flow;
        this.output = output;
        this.errorOutput = errorOutput;
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
    }

    @Override
    public <OT> FlowBuilder<I, OT> appendFlow(Flow<O, OT> flow) {
        return new FlowBuilderImpl<>(this.flow.composite(flow), output, errorOutput, exceptionHandlers);
    }

    @Override
    public <OT> FlowBuilder<I, O> ifThen(Predicate<O> tester, Flow<O, OT> flow) {
        return new FlowBuilderImpl<>(this.flow.composite(input -> {
            if (tester.test(input.value())) {
                flow.call(input);
            }
            return input;
        }), output, errorOutput, exceptionHandlers);
    }

    @Override
    public <QT> FlowBuilder<I, QT> question(String questionText, List<QuestionAnswer<QT>> questionAnswers) {
        return null;
    }

    @Override
    public FlowBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public FlowBuilder<I, O> output(PrintWriter output) {
        this.output = output;
        return this;
    }

    @Override
    public FlowBuilder<I, O> errorOutput(PrintWriter errorOutput) {
        this.errorOutput = errorOutput;
        return this;
    }


    @Override
    public Flow<I, O> build() {
        return new FlowImpl<>(flow, exceptionHandlers, output, errorOutput);
    }

    private static Charset getStdoutEncoding() {
        String encoding = System.getProperty("sun.stdout.encoding");
        return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
    }
}
