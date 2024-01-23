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

package org.apache.ignite.ddl.sql;

import org.apache.ignite.ddl.Options;

class QueryContext {

    final Options options;
    final StringBuilder sql;
    private int indent;
    private boolean newline = false;

    QueryContext(Options options) {
        this.options = options;
        this.sql = new StringBuilder();
    }


    /**
     * Visit a QueryPart in the current context.
     * This method is called by certain QueryPart implementations to recursively visit nested parts.
     *
     * @param part The component.
     * @return current context.
     * @see AbstractDdlQuery
     */
    QueryContext visit(QueryPart part) {
        part.accept(this);
        return this;
    }

    String getSql() {
        return sql.toString();
    }

    QueryContext sql(String s) {
        applyNewLine();
        this.sql.append(s);
        resetSeparatorFlags();
        return this;
    }

    QueryContext sql(int i) {
        applyNewLine();
        this.sql.append(i);
        resetSeparatorFlags();
        return this;
    }

    QueryContext sql(double d) {
        applyNewLine();
        this.sql.append(d);
        resetSeparatorFlags();
        return this;
    }

    QueryContext formatSeparator() {
        if (!newline && options.isPrettyPrint()) {
            newline = true;
        }
        return this;
    }

    boolean isQuoteNames() {
        return options.isQuoteIdentifiers();
    }

    QueryContext sqlIndentStart(String s) {
        return sql(s).sqlIndentStart();
    }

    private QueryContext sqlIndentStart() {
        indent += options.getIndentWidth();
        if (options.isPrettyPrint()) {
            newline = true;
        }
        return this;
    }

    QueryContext sqlIndentEnd(String s) {
        return sqlIndentEnd().sql(s);
    }

    private QueryContext sqlIndentEnd() {
        indent -= options.getIndentWidth();
        return this.formatSeparator();
    }


    private String indentation() {
        return indent == 0 ? "" : " ".repeat(indent);
    }

    private void applyNewLine() {
        if (newline) {
            sql.append("\n");
            sql.append(indentation());
        }
    }

    private void resetSeparatorFlags() {
        newline = false;
    }

}
