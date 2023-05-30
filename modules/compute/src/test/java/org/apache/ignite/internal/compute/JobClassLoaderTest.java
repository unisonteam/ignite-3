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

package org.apache.ignite.internal.compute;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URL;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobClassLoaderTest {

    @Mock
    private ClassLoader parentClassLoader;

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of("java.lang.String", String.class),
                Arguments.of("javax.lang.String", String.class),
                Arguments.of("org.apache.ignite.internal.compute.JobExecutionContextImpl", JobExecutionContextImpl.class)
        );
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void loadsSystemClassesFirst(String className, Class<?> desiredClass) throws Exception {

        doReturn(desiredClass).when(parentClassLoader).loadClass(className);

        try (TestJobClassLoader jobClassLoader = spy(new TestJobClassLoader(new URL[0], parentClassLoader))) {
            assertSame(desiredClass, jobClassLoader.loadClass(className));
            verify(jobClassLoader, never()).findClass(className);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"org.apache.ignite.compute.unit1.UnitJob", "java.lang.String", "javax.lang.String"})
    public void loadsOwnClassIfSystemAbsent(String className) throws Exception {
        doThrow(ClassNotFoundException.class).when(parentClassLoader).loadClass(className);

        try (TestJobClassLoader jobClassLoader = spy(new TestJobClassLoader(new URL[0], parentClassLoader))) {
            Class<TestJobClassLoader> toBeReturned = TestJobClassLoader.class;
            doReturn(toBeReturned).when(jobClassLoader).findClass(className);

            assertSame(toBeReturned, jobClassLoader.loadClass(className));
            verify(parentClassLoader, times(1)).loadClass(className);
        }
    }

    private static class TestJobClassLoader extends JobClassLoader {
        TestJobClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }
    }
}
