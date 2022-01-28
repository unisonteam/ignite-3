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

package org.apache.ignite.rest;

import org.apache.ignite.rest.exception.ConfigPathUnrecognizedException;
import org.apache.ignite.rest.exception.InvalidConfigFormatException;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;

/**
 * Base configuration controller.
 */
public class AbstractConfigurationController {
    /** Presentation of the configuration. */
    private final ConfigurationPresentation<String> cfgPresentation;
    
    public AbstractConfigurationController(ConfigurationPresentation<String> clusterCfgPresentation) {
        this.cfgPresentation = clusterCfgPresentation;
    }
    
    public String configuration() {
        return this.cfgPresentation.represent();
    }
    
    public String getConfigurationByPath(String path) {
        try {
            return cfgPresentation.representByPath(path);
        } catch (IllegalArgumentException ex) {
            throw new ConfigPathUnrecognizedException(ex);
        }
    }
    
    public void updateConfiguration(String updatedConfiguration) {
        try {
            cfgPresentation.update(updatedConfiguration);
        } catch (IllegalArgumentException ex) {
            throw new InvalidConfigFormatException(ex);
        }
    }
}
