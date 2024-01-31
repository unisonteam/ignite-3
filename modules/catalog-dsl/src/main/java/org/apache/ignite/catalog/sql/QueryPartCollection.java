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

package org.apache.ignite.catalog.sql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

class QueryPartCollection<T extends QueryPart> extends QueryPart implements Collection<T> {

    private final Collection<T> wrapped;

    private String separator = ", ";

    private boolean formatSeparator = false;

    public static <T extends QueryPart> QueryPartCollection<T> wrap(List<T> wrapped) {
        return new QueryPartCollection<>(wrapped);
    }

    @SafeVarargs
    public static <T extends QueryPart> QueryPartCollection<T> wrap(T... wrapped) {
        return new QueryPartCollection<>(wrapped);
    }

    private QueryPartCollection(Collection<T> wrapped) {
        this.wrapped = wrapped;
    }

    @SafeVarargs
    private QueryPartCollection(T... wrapped) {
        this.wrapped = Arrays.asList(wrapped);
    }


    public QueryPartCollection<T> separator(String separator) {
        this.separator = separator;
        return this;
    }

    public QueryPartCollection<T> formatSeparator() {
        return formatSeparator(true);
    }

    public QueryPartCollection<T> formatSeparator(boolean b) {
        this.formatSeparator = b;
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        var first = true;
        for (var part : wrapped) {
            if (!first) {
                ctx.sql(separator);
                if (formatSeparator) {
                    ctx.formatSeparator();
                }
            }
            ctx.visit(part);
            first = false;
        }
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return wrapped.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return wrapped.iterator();
    }

    @Override
    public Object[] toArray() {
        return wrapped.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return wrapped.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return wrapped.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return wrapped.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return wrapped.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return wrapped.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return wrapped.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return wrapped.retainAll(c);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }
}
