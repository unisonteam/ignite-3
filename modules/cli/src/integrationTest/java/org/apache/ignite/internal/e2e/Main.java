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

import com.google.common.base.Ascii;
import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
//        Process process = new ProcessBuilder("sh", "bin/ignite3")
//                .directory(Path.of("/Users/apakhomov/unison/ignite-3/packaging/build/distributions/ignite3-cli-3.0.0-SNAPSHOT").toFile()
//        ).start();
//        System.out.println("wait: " + process.waitFor());

        var envs = System.getenv();
        envs = new HashMap<>(System.getenv());
        envs.put("TERM", "xterm-256color");

        String[] cmd = {"/bin/sh"};
        PtyProcess process = new PtyProcessBuilder().setCommand(cmd)
                .setEnvironment(envs)
                .setDirectory("/Users/apakhomov/unison/ignite-3/packaging/cli/build/distributions/ignite3-cli-3.0.0-SNAPSHOT")
                .setConsole(false).start();
        var ptyInput = process.getOutputStream();

//        AsyncOutputConsumer asyncStdout = new AsyncOutputConsumer(new OutputConsumer("stdout", process.getInputStream()));
//        asyncStdout.consume();
//
//        AsyncOutputConsumer asyncStderr = new AsyncOutputConsumer(new OutputConsumer("stderr", process.getErrorStream()));
//        asyncStderr.consume();

        var stdout = startStdoutGobbler(process);

        Scanner sc = new Scanner(System.in);
        String command = null;
        while (true) {
            System.out.println(":>");
            command = sc.nextLine();
            writeToStdinAndFlush(process, command, true);
            Thread.sleep(100);
            System.out.println("stdout: " + stdout.getOutput());
        }
    }

    static class AsyncOutputConsumer {
        private final OutputConsumer outputConsumer;
        private Thread thread;

        AsyncOutputConsumer(OutputConsumer outputConsumer) {
            this.outputConsumer = outputConsumer;
        }

        void consume() {
            thread = new Thread(() -> {
                try {
                    outputConsumer.consume();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
        }

        List<String> readLines() {
            return outputConsumer.readLines();
        }
    }

    static class OutputConsumer {

        private final InputStreamReader br;

        private final CopyOnWriteArrayList<String> lines = new CopyOnWriteArrayList();

        private final String name;

        OutputConsumer(String name, InputStream is) {
            this.name = name;
            this.br = new InputStreamReader(is);
        }

        void consume() throws IOException, InterruptedException {
            char[] buf = new char[32 * 1024];
            while (true) {
                int count = br.read(buf);
                if (count <= 0) {
                    br.close();
                    return;
                }
                String line = new String(buf, 0, count);
                lines.add(line);
                System.out.println(this.name + ": " + line);
                Thread.sleep(100);
            }
        }

        List<String> readLines() {
            return lines;
        }
    }

    public static void writeToStdinAndFlush(PtyProcess process, String input, boolean hitEnter) throws IOException {
        String text = hitEnter ? input + (char) process.getEnterKeyCode() : input;
        process.getOutputStream().write(text.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().flush();
    }

    public static Gobbler startStdoutGobbler(PtyProcess process) {
        return new Gobbler(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8), null, process);
    }

    public static Gobbler startStderrGobbler(PtyProcess process) {
        return new Gobbler(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8), null, process);
    }


    private static Gobbler startReader(InputStream in, CountDownLatch latch) {
        return new Gobbler(new InputStreamReader(in, StandardCharsets.UTF_8), latch, null);
    }

    public static class Gobbler implements Runnable {
        private final Reader myReader;
        private final CountDownLatch myLatch;
        private final PtyProcess myProcess;
        private final StringBuffer myOutput;
        private final Thread myThread;
        private final BlockingQueue<String> myLineQueue = new LinkedBlockingQueue<>();
        private final ReentrantLock myNewTextLock = new ReentrantLock();
        private final Condition myNewTextCondition = myNewTextLock.newCondition();

        private Gobbler(Reader reader, CountDownLatch latch, PtyProcess process) {
            myReader = reader;
            myLatch = latch;
            myProcess = process;
            myOutput = new StringBuffer();
            myThread = new Thread(this, "Stream gobbler");
            myThread.start();
        }

        @Override
        public void run() {
            try {
                char[] buf = new char[32 * 1024];
                String linePrefix = "";
                while (true) {
                    int count = myReader.read(buf);
                    if (count <= 0) {
                        myReader.close();
                        return;
                    }
                    myOutput.append(buf, 0, count);
                    linePrefix = processLines(linePrefix + new String(buf, 0, count));
                    myNewTextLock.lock();
                    try {
                        myNewTextCondition.signalAll();
                    }
                    finally {
                        myNewTextLock.unlock();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (myLatch != null) {
                    myLatch.countDown();
                }
            }
        }

        private String processLines(String text) {
            int start = 0;
            while (true) {
                int end = text.indexOf('\n', start);
                if (end < 0) {
                    return text.substring(start);
                }
                myLineQueue.add(text.substring(start, end + 1));
                start = end + 1;
            }
        }

        public String getOutput() {
            return myOutput.toString();
        }

        public void awaitFinish() throws InterruptedException {
            myThread.join(TimeUnit.SECONDS.toMillis(10));
        }

        public String readLine() throws InterruptedException {
            return readLine(TimeUnit.SECONDS.toMillis(10));
        }

        public String readLine(long awaitTimeoutMillis) throws InterruptedException {
            String line = myLineQueue.poll(awaitTimeoutMillis, TimeUnit.MILLISECONDS);
            if (line != null) {
                line = cleanWinText(line);
            }
            return line;
        }

        private boolean awaitTextEndsWith(String suffix, long timeoutMillis) {
            long startTimeMillis = System.currentTimeMillis();
            long nextTimeoutMillis = timeoutMillis;
            do {
                myNewTextLock.lock();
                try {
                    try {
                        if (endsWith(suffix)) {
                            return true;
                        }
                        myNewTextCondition.await(nextTimeoutMillis, TimeUnit.MILLISECONDS);
                        if (endsWith(suffix)) {
                            return true;
                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                        return false;
                    }
                }
                finally {
                    myNewTextLock.unlock();
                }
                nextTimeoutMillis = startTimeMillis + timeoutMillis - System.currentTimeMillis();
            } while (nextTimeoutMillis >= 0);
            return false;
        }

        private boolean endsWith(String suffix) {
            String text = cleanWinText(myOutput.toString());
            return text.endsWith(suffix);
        }

        private static String cleanWinText(String text) {
//            if (Platform.isWindows()) {
//                text = text.replace("\u001B[0m", "").replace("\u001B[m", "").replace("\u001B[0K", "").replace("\u001B[K", "")
//                        .replace("\u001B[?25l", "").replace("\u001b[?25h", "").replaceAll("\u001b\\[\\d*G", "")
//                        .replace("\u001b[2J", "").replaceAll("\u001B\\[\\d*;?\\d*H", "")
//                        .replaceAll("\u001B\\[\\d*X", "")
//                        .replaceAll(" *\\r\\n", "\r\n").replaceAll(" *$", "").replaceAll("(\\r\\n)+\\r\\n$", "\r\n");
//                int oscInd = 0;
//                do {
//                    oscInd = text.indexOf("\u001b]0;", oscInd);
//                    int bellInd = oscInd >= 0 ? text.indexOf(Ascii.BEL, oscInd) : -1;
//                    if (bellInd >= 0) {
//                        text = text.substring(0, oscInd) + text.substring(bellInd + 1);
//                    }
//                } while (oscInd >= 0);
//                int backspaceInd = text.indexOf(Ascii.BS);
//                while (backspaceInd >= 0) {
//                    text = text.substring(0, Math.max(0, backspaceInd - 1)) + text.substring(backspaceInd + 1);
//                    backspaceInd = text.indexOf(Ascii.BS);
//                }
//            }
            return text;
        }

//        public void assertEndsWith(String expectedSuffix) {
//            assertEndsWith(expectedSuffix, TimeUnit.SECONDS.toMillis(10000));
//        }

//        private void assertEndsWith(String expectedSuffix, long timeoutMillis) {
//            boolean ok = awaitTextEndsWith(expectedSuffix, timeoutMillis);
//            if (!ok) {
//                String output = getOutput();
//                String cleanOutput = cleanWinText(output);
//                String actual = cleanOutput.substring(Math.max(0, cleanOutput.length() - expectedSuffix.length()));
//                if (expectedSuffix.equals(actual)) {
//                    fail("awaitTextEndsWith could detect suffix within timeout, but it is there");
//                }
//                expectedSuffix = convertInvisibleChars(expectedSuffix);
//                actual = convertInvisibleChars(actual);
//                int lastTextSize = 1000;
//                String lastText = output.substring(Math.max(0, output.length() - lastTextSize));
//                if (output.length() > lastTextSize) {
//                    lastText = "..." + lastText;
//                }
//                assertEquals("Unmatched suffix (trailing text: " + convertInvisibleChars(lastText) +
//                        (myProcess != null ? ", " + getProcessStatus(myProcess) : "") + ")", expectedSuffix, actual);
//                fail("Unexpected failure");
//            }
//        }
    }
}
