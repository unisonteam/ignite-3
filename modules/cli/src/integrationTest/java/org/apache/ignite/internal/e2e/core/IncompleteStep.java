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

package org.apache.ignite.internal.e2e.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

public class IncompleteStep {
    private final Input input;
    private final Queue<String> out;
    private final Queue<String> err;
    private int exitCode;

    public IncompleteStep(Input input) {
        this.input = input;
        this.out = new ArrayDeque<>();
        this.err = new ArrayDeque<>();
    }

    public void outAdd(String line) {
        out.add(line);
    }

    public void errAdd(String line) {
        err.add(line);
    }

    public CompleteStep complete() {
        return new CompleteStep(input, new Output(toList(out), toList(err), exitCode));
    }

    private List<String> toList(Queue<String> queue) {
        List<String> res = new ArrayList<>();
        String line = queue.poll();
        while (line != null) {
            res.add(line);
            line = queue.poll();
        }

        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IncompleteStep)) {
            return false;
        }
        IncompleteStep that = (IncompleteStep) o;
        return exitCode == that.exitCode && Objects.equals(input, that.input) && Objects.equals(out, that.out)
                && Objects.equals(err, that.err);
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, out, err, exitCode);
    }

    @Override
    public String toString() {
        return "IncompleteStep{" +
                "input=" + input +
                ", out=" + out +
                ", err=" + err +
                ", exitCode=" + exitCode +
                '}';
    }
}
