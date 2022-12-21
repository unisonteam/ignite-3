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

import static org.junit.jupiter.api.Assertions.fail;

import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import picocli.CommandLine.Help.Ansi;

public class Cli {
    private final PtyProcess process;
    private final Gobbler sout;
    private final Gobbler serr;
    private int lastExitCode;

    private Cli(PtyProcess process) {
        this.process = process;
        this.sout = startStdoutGobbler(process);
        this.serr = startStdoutGobbler(process);
    }

    public static Cli startInteractive(Path workDir) {
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
        return new Cli(process);
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
        return new Gobbler(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8), null, process);
    }

    public static Gobbler startStderrGobbler(PtyProcess process) {
        return new Gobbler(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8), null, process);
    }

    public Cli run(String cmd) {
        try {
            writeToStdinAndFlush(this.process, cmd, true);
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Cli assertThatOutput(Matcher<String> matcher) {
        if (!awaitMatches(matcher)) {
//            matcher.describeMismatch(sout.getOutput(), Description.NONE);
            fail("Expecting " + matcher + " but was: " + sout.getOutput());
        }

        return this;
    }

    String readCleanText() throws InterruptedException {
        String line = sout.readLine();
        if (line != null) {
            line = Ansi.OFF.string(line);
        }

        return line;
    }

    boolean awaitMatches(Matcher<String> matcher) {
        ArrayList<String> processedLines = new ArrayList<>();

        long waitTime = Duration.ofSeconds(20).toNanos();
        long startNanos = System.nanoTime();
        String line = null;
        while (startNanos + waitTime > System.nanoTime()) {
            try {
                line = readCleanText();
                processedLines.add(line);
                while (line != null) {
                    if (matcher.matches(line)) {
                        return true;
                    }
                    line = readCleanText();
                    processedLines.add(line);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return false;
    }

    public void stop() {
        System.out.println("***" + process.getPid());
        process.destroyForcibly();
    }
}
