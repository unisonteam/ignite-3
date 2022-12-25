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

import java.util.List;
import java.util.Objects;

public class Output {
    private final List<String> out;
    private final List<String> err;
    private final int exit;

    public Output(List<String> out, List<String> err, int exit) {
        this.out = out;
        this.err = err;
        this.exit = exit;
    }

    public List<String> out() {
        return out;
    }

    public List<String> err() {
        return err;
    }

    public int exit() {
        return exit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Output)) {
            return false;
        }
        Output output = (Output) o;
        return exit == output.exit && Objects.equals(out, output.out) && Objects.equals(err, output.err);
    }

    @Override
    public int hashCode() {
        return Objects.hash(out, err, exit);
    }
}
