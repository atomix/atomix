/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.config.jackson.impl;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;

/**
 * Property naming strategy that removes the "Config" suffix from properties.
 */
public class ConfigPropertyNamingStrategy extends PropertyNamingStrategy.KebabCaseStrategy {
  private static final String CONFIG_SUFFIX = "Config";

  @Override
  public String translate(String input) {
    if (input.endsWith(CONFIG_SUFFIX)) {
      return super.translate(input.substring(0, input.length() - CONFIG_SUFFIX.length()));
    }
    return super.translate(input);
  }
}