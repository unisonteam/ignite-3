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

package org.apache.ignite.catalog;

public class Options {

    private boolean prettyPrint = false;
    private boolean quoteIdentifiers = false;
    private int indentWidth = 2;

    public Options() {
    }

    public static Options defaultOptions() {
        return new Options();
    }

    public Options prettyPrint() {
        return prettyPrint(true);
    }

    public Options prettyPrint(boolean b) {
        this.prettyPrint = b;
        return this;
    }

    public Options quoteIdentifiers() {
        return quoteIdentifiers(true);
    }

    public Options quoteIdentifiers(boolean b) {
        this.quoteIdentifiers = b;
        return this;
    }

    public Options indentWidth(int width) {
        this.indentWidth = width;
        return this;
    }

    public boolean isPrettyPrint() {
        return prettyPrint;
    }

    public boolean isQuoteIdentifiers() {
        return quoteIdentifiers;
    }

    public int getIndentWidth() {
        return indentWidth;
    }
}
