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
import org.apache.ignite.cli.core.decorator.TerminalOutput;
import org.apache.ignite.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.cli.commands.decorators.DefaultDecoratorRegistry;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.flow.FlowInterruptException;
import org.apache.ignite.cli.core.flow.Flowable;

public class FlowImpl<I, O> implements Flow<I, O> {
    private final Flow<I, O> flow;

    /**
     * Decorator that decorates call's output.
     */
    private final DecoratorRegistry store = new DefaultDecoratorRegistry();

    /**
     * Handlers for any exceptions.
     */
    private final ExceptionHandlers exceptionHandlers;

    private final PrintWriter output;
    private final PrintWriter errorOutput;

    FlowImpl(Flow<I, O> flow, ExceptionHandlers exceptionHandlers, PrintWriter output, PrintWriter errorOutput) {
        this.flow = flow;
        this.exceptionHandlers = exceptionHandlers;
        this.output = output;
        this.errorOutput = errorOutput;
    }

    @Override
    public Flowable<O> call(Flowable<I> input) {

        Flowable<O> output;
        try {
            output = flow.call(input);
        } catch (FlowInterruptException e) {
            output = Flowable.empty();
        }

        if (output.hasError()) {
            PrintWriter pw = errorOutput != null ? errorOutput : new PrintWriter(System.err);
            exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(pw), output.errorCause());
        }

        if (output.hasResult() && this.output != null) {
            TerminalOutput decoratedOutput = store.getDecorator(output.type()).decorate(output.value());
            this.output.println(decoratedOutput.toTerminalString());
        }
        return output;
    }
}
