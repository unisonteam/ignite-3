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

package org.apache.ignite.internal.eventlog.config;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.eventlog.config.schema.EventLogExtensionConfigurationSchema;
import org.apache.ignite.internal.eventlog.config.schema.EventTypeValidatorImpl;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkConfigurationSchema;
import org.apache.ignite.internal.eventlog.config.schema.WebhookSinkConfigurationSchema;

/**
 * {@link ConfigurationModule} for cluster-wide configuration provided by eventlog.
 */
@AutoService(ConfigurationModule.class)
public class EventLogConfigurationModule implements ConfigurationModule {
    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    @Override
    public Collection<Class<?>> schemaExtensions() {
        return List.of(EventLogExtensionConfigurationSchema.class);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(LogSinkConfigurationSchema.class, WebhookSinkConfigurationSchema.class);
    }

    /** {@inheritDoc} */
    @Override
    public Set<Validator<?, ?>> validators() {
        return Set.of(EventTypeValidatorImpl.INSTANCE);
    }
}
