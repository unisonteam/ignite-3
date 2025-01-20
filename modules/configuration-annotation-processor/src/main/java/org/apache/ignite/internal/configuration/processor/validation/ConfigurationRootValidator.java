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

package org.apache.ignite.internal.configuration.processor.validation;

import javax.annotation.processing.ProcessingEnvironment;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.processor.ClassWrapper;

/**
 * Validator for the {@link ConfigurationRoot} annotation.
 */
public class ConfigurationRootValidator extends Validator {
    public ConfigurationRootValidator(ProcessingEnvironment processingEnvironment) {
        super(processingEnvironment);
    }

    @Override
    public void validate(ClassWrapper classWrapper) {
        if (classWrapper.getAnnotation(ConfigurationRoot.class) == null) {
            return;
        }

        assertHasCompatibleTopLevelAnnotation(classWrapper, ConfigurationRoot.class, ConfigurationExtension.class);

        assertNotContainsFieldAnnotatedWith(classWrapper, PolymorphicId.class);

        ClassWrapper superClass = classWrapper.superClass();

        if (superClass == null) {
            return;
        }

        assertSuperclassHasAnnotations(classWrapper, AbstractConfiguration.class);

        assertNoFieldNameConflictsWithSuperClass(classWrapper);

        assertSuperClassNotContainsFieldAnnotatedWith(classWrapper, InjectedName.class, InternalId.class);
    }
}
