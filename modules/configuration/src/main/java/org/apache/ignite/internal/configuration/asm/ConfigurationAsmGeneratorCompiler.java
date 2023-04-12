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

package org.apache.ignite.internal.configuration.asm;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.collectSchemas;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.isPolymorphicId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicInstanceId;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaFields;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

public class ConfigurationAsmGeneratorCompiler {

    private final Collection<RootKey<?, ?>> rootKeys;

    private final Map<Class<?>, Set<Class<?>>> internalExtensions;

    private final Map<Class<?>, Set<Class<?>>> polymorphicExtensions;

    /**
     * @param rootKeys Configuration root keys.
     * @param internalSchemaExtensions Internal extensions ({@link InternalConfiguration}) of configuration schemas
     *         ({@link ConfigurationRoot} and {@link Config}).
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas.
     */
    public ConfigurationAsmGeneratorCompiler(
            Collection<RootKey<?, ?>> rootKeys,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions) {

        Set<Class<?>> allSchemas = collectAllSchemas(rootKeys, internalSchemaExtensions, polymorphicSchemaExtensions);

        this.internalExtensions = internalExtensionsWithCheck(allSchemas, internalSchemaExtensions);
        this.polymorphicExtensions = polymorphicExtensionsWithCheck(allSchemas, polymorphicSchemaExtensions);

        this.rootKeys = rootKeys;
    }

    /**
     * Collects all schemas and subschemas (recursively) from root keys, internal and polymorphic schema extensions.
     *
     * @param rootKeys root keys
     * @param internalSchemaExtensions internal schema extensions
     * @param polymorphicSchemaExtensions polymorphic schema extensions
     * @return set of all schema classes
     */
    private static Set<Class<?>> collectAllSchemas(Collection<RootKey<?, ?>> rootKeys,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions) {
        Set<Class<?>> allSchemas = new HashSet<>();

        allSchemas.addAll(collectSchemas(viewReadOnly(rootKeys, RootKey::schemaClass)));
        allSchemas.addAll(collectSchemas(internalSchemaExtensions));
        allSchemas.addAll(collectSchemas(polymorphicSchemaExtensions));

        return allSchemas;
    }

    public void compile(ConfigurationAsmGenerator generator) {
        rootKeys.forEach(key -> generator.compileRootSchema(key.schemaClass(), internalExtensions, polymorphicExtensions));
    }

    /**
     * Get configuration schemas and their validated internal extensions with checks.
     *
     * @param allSchemas All configuration schemas.
     * @param internalSchemaExtensions Internal extensions ({@link InternalConfiguration}) of configuration schemas
     *         ({@link ConfigurationRoot} and {@link Config}).
     * @return Mapping: original of the schema -> internal schema extensions.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private Map<Class<?>, Set<Class<?>>> internalExtensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> internalSchemaExtensions
    ) {
        if (internalSchemaExtensions.isEmpty()) {
            return Map.of();
        }

        Map<Class<?>, Set<Class<?>>> internalExtensions = internalSchemaExtensions(internalSchemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(internalExtensions.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Internal extensions for which no parent configuration schemas were found: " + notInAllSchemas
            );
        }

        return internalExtensions;
    }

    /**
     * Get polymorphic extensions of configuration schemas with checks.
     *
     * @param allSchemas All configuration schemas.
     * @param polymorphicSchemaExtensions Polymorphic extensions ({@link PolymorphicConfigInstance}) of configuration schemas.
     * @return Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If the schema extension is invalid.
     */
    private Map<Class<?>, Set<Class<?>>> polymorphicExtensionsWithCheck(
            Set<Class<?>> allSchemas,
            Collection<Class<?>> polymorphicSchemaExtensions
    ) {
        Map<Class<?>, Set<Class<?>>> polymorphicExtensionsByParent = polymorphicSchemaExtensions(polymorphicSchemaExtensions);

        Set<Class<?>> notInAllSchemas = difference(polymorphicExtensionsByParent.keySet(), allSchemas);

        if (!notInAllSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic extensions for which no polymorphic configuration schemas were found: " + notInAllSchemas
            );
        }

        Collection<Class<?>> noPolymorphicExtensionsSchemas = allSchemas.stream()
                .filter(ConfigurationUtil::isPolymorphicConfig)
                .filter(not(polymorphicExtensionsByParent::containsKey))
                .collect(toList());

        if (!noPolymorphicExtensionsSchemas.isEmpty()) {
            throw new IllegalArgumentException(
                    "Polymorphic configuration schemas for which no extensions were found: " + noPolymorphicExtensionsSchemas
            );
        }

        checkPolymorphicConfigIds(polymorphicExtensionsByParent);

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensionsByParent.entrySet()) {
            Class<?> schemaClass = e.getKey();

            Field typeIdField = schemaFields(schemaClass).get(0);

            if (!isPolymorphicId(typeIdField)) {
                throw new IllegalArgumentException(String.format(
                        "First field in a polymorphic configuration schema must contain @%s: %s",
                        PolymorphicId.class,
                        schemaClass.getName()
                ));
            }
        }

        return polymorphicExtensionsByParent;
    }


    /**
     * Checks that there are no conflicts between ids of a polymorphic configuration and its extensions (instances).
     *
     * @param polymorphicExtensions Mapping: polymorphic scheme -> extensions (instances) of polymorphic configuration.
     * @throws IllegalArgumentException If a polymorphic configuration id conflict is found.
     * @see PolymorphicConfigInstance#value
     */
    private void checkPolymorphicConfigIds(Map<Class<?>, Set<Class<?>>> polymorphicExtensions) {
        // Mapping: id -> configuration schema.
        Map<String, Class<?>> ids = new HashMap<>();

        for (Map.Entry<Class<?>, Set<Class<?>>> e : polymorphicExtensions.entrySet()) {
            for (Class<?> schemaClass : e.getValue()) {
                String id = polymorphicInstanceId(schemaClass);
                Class<?> prev = ids.put(id, schemaClass);

                if (prev != null) {
                    throw new IllegalArgumentException("Found an id conflict for a polymorphic configuration [id="
                            + id + ", schemas=" + List.of(prev, schemaClass));
                }
            }

            ids.clear();
        }
    }

}
