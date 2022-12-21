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

import com.pty4j.PtyProcess;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Gobbler implements Runnable {
    private final Reader reader;
    private final StringBuffer output;
    private final Thread thread;
    private final ConcurrentLinkedQueue<String> lineQueue = new ConcurrentLinkedQueue<>();

    Gobbler(Reader reader, CountDownLatch latch, PtyProcess process) {
        this.reader = reader;
        output = new StringBuffer();
        thread = new Thread(this, "Stream gobbler");
        thread.start();
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

    @Override
    public void run() {
        try {
            int maxCharBufferSize = 2048;
            String linePrefix = "";
            while (true) {
                char[] buf = new char[32 * maxCharBufferSize];
                int count = reader.read(buf);
                if (count <= 0) {
                    reader.close();
                    return;
                }
                output.append(buf, 0, count);
                System.out.println("######## [" + count + "]: " + new String(buf, 0, count));
                lineQueue.add(new String(buf, 0, count));
//                linePrefix = processLines(linePrefix + new String(buf, 0, count));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String processLines(String text) {
        int start = 0;
        while (true) {
            int end = text.indexOf('\n', start);
            if (end < 0) {
                String lastLine = text.substring(start);
                System.out.println("######## [append to queue]: " + lastLine);
                lineQueue.add(lastLine);
                return lastLine;
            }
            System.out.println("######## [append to queue]: " + text.substring(start, end + 1));
            lineQueue.add(text.substring(start, end + 1));
            start = end + 1;
        }
    }

    public String getOutput() {
        return output.toString();
    }

    public void awaitFinish() throws InterruptedException {
        thread.join(Duration.ofSeconds(10).toMillis());
    }

    public String readLine() throws InterruptedException {
        return readLine(Duration.ofSeconds(10).toMillis());
    }

    public String readLine(long awaitTimeoutMillis) throws InterruptedException {
        String line = lineQueue.poll();
        if (line != null) {
            System.out.println("######## [read]: " + line);
            line = cleanWinText(line);
        }
        return line;
    }
}
