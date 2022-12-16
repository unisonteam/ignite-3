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
import java.util.ArrayList;
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
import java.util.stream.Collectors;

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
                .setDirectory("/Users/apakhomov/unison/ignite-3/packaging/build/distributions/ignite3-cli-3.0.0-SNAPSHOT")
                .setConsole(false).start();
        var ptyInput = process.getOutputStream();

//        AsyncOutputConsumer asyncStdout = new AsyncOutputConsumer(new OutputConsumer("stdout", process.getInputStream()));
//        asyncStdout.consume();
//
//        AsyncOutputConsumer asyncStderr = new AsyncOutputConsumer(new OutputConsumer("stderr", process.getErrorStream()));
//        asyncStderr.consume();

//        var stdout = startStdoutGobbler(process);
//
//        Scanner sc = new Scanner(System.in);
//        String command = null;
//        while (true) {
//            System.out.print(">");
//            command = sc.nextLine();
//            if (command.endsWith("\t")) {
//                writeToStdinHitTabAfter(process, command, true);
//            } else {
//                writeToStdinAndFlush(process, command, true);
//            }
//            List<String> cmdResult = fetchCommandOutput(stdout);
//            System.out.println(String.join("", cmdResult));
//        }
    }
}
