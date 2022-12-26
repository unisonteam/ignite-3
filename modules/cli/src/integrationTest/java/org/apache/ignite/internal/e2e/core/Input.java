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

import java.util.Objects;

public class Input {
    private final String text;
    private final boolean hitEnter;

    public Input(String text, boolean hitEnter) {
        this.text = text;
        this.hitEnter = hitEnter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Input)) {
            return false;
        }
        Input input = (Input) o;
        return hitEnter == input.hitEnter && Objects.equals(text, input.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, hitEnter);
    }

    @Override
    public String toString() {
        return "Input{" +
                "text='" + text + '\'' +
                ", hitEnter=" + hitEnter +
                '}';
    }
}
