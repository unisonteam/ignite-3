/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.e2e;

import static org.hamcrest.MatcherAssert.assertThat;

import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.e2e.core.CompleteStep;
import org.apache.ignite.internal.e2e.core.History;
import org.apache.ignite.internal.e2e.core.IncompleteStep;
import org.apache.ignite.internal.e2e.core.Input;
import org.apache.ignite.internal.e2e.core.StepCompleter;
import org.hamcrest.Matcher;
import picocli.CommandLine.Help.Ansi;

public class Cli {
    private final History history;
    private final PtyProcess process;
    private final Gobbler sout;
    private final Gobbler serr;
    private int lastExitCode;

    private Cli(PtyProcess process, Consumer<String> logger) {
        this.history = History.empty(logger);
        this.process = process;
        this.sout = startStdoutGobbler(process); // must be only single gobbler per process, not per output stream
        this.serr = null;
    }

    public static Cli startInteractive(Path workDir, Consumer<String> logger) {
        var envs = System.getenv();
        envs = new HashMap<>(System.getenv());
        envs.put("TERM", "xterm-256color");

        String[] cmd = {"/bin/sh"};
        PtyProcess process = null;
        try {
            process = new PtyProcessBuilder().setCommand(cmd)
                    .setEnvironment(envs)
                    .setDirectory(workDir.toString())
                    .setConsole(true).start();
        } catch (IOException e) {
            throw new RuntimeException(e); // todo
        }
        return new Cli(process, logger);
    }


    public static void writeToStdinAndFlush(PtyProcess process, String input, boolean hitEnter) throws IOException {
        String text = hitEnter ? input + (char) process.getEnterKeyCode() : input;
        process.getOutputStream().write(text.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().flush();
    }

    public static void writeToStdinHitTabAfter(PtyProcess process, String input, boolean hitEnter) throws IOException {
        String text = input + '\t' + (char) process.getEnterKeyCode();
        process.getOutputStream().write(text.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().flush();
    }

    public static Gobbler startStdoutGobbler(PtyProcess process) {
        // close streams
        return new Gobbler(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8), process);
    }

    public static Gobbler startStderrGobbler(PtyProcess process) {
        return new Gobbler(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8), process);
    }

    public Cli run(String cmd) {
        try {
            var input = new Input(cmd, true);
            var step = new IncompleteStep(input);
            history.run(step);
            writeToStdinAndFlush(this.process, cmd, true);
            new StepCompleter(step, history, serr, sout).completeAsync();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Cli assertThatOutput(Matcher<String> matcher) {
        String text = readCleanTextBlocking();
        try {
            assertThat(text, matcher);
        } catch (Throwable e) {
            throw e;
        }
        return this;
    }

    String readCleanTextBlocking() {
        history.waitStepCompleted();
        CompleteStep lastStep = history.findLastStep();

        return Ansi.OFF.string(lastStep.output().out().stream().collect(Collectors.joining("\n")));
    }

    public void stop() {
        System.out.println("***" + process.getPid());
        process.destroyForcibly();
    }
}
