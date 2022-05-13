/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli;

import io.micronaut.configuration.picocli.MicronautFactory;
import java.util.HashMap;
import org.apache.ignite.cli.commands.TopLevelCliCommand;
import org.apache.ignite.cli.commands.TopLevelCliReplCommand;
import org.apache.ignite.cli.core.repl.Repl;
import org.apache.ignite.cli.core.repl.executor.ReplExecutor;
import picocli.CommandLine;

/**
 * Ignite cli entry point.
 */
public class Main {
    /**
     * Entry point.
     *
     * @param args ignore.
     */
    public static void main(String[] args) {
        try (MicronautFactory micronautFactory = new MicronautFactory()) {
            if (args.length != 0) {
                executeCommand(args, micronautFactory);
            } else {
                try {
                    enterRepl(micronautFactory);
                } catch (Exception e) {
                    System.err.println("Error occurred during REPL initialization");
                }
            }
        }
    }

    private static void enterRepl(MicronautFactory micronautFactory) throws Exception {
        ReplExecutor replExecutor = micronautFactory.create(ReplExecutor.class);
        replExecutor.injectFactory(micronautFactory);
        HashMap<String, String> aliases = new HashMap<>();
        aliases.put("zle", "widget");
        aliases.put("bindkey", "keymap");

        replExecutor.execute(Repl.builder()
                .withAliases(aliases)
                .withCommandClass(TopLevelCliReplCommand.class)
                .build());
    }

    private static void executeCommand(String[] args, MicronautFactory micronautFactory) {
        new CommandLine(TopLevelCliCommand.class, micronautFactory).execute(args);
    }
}
