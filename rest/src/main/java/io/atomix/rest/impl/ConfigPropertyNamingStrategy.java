// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.impl;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;

/**
 * Property naming strategy that removes the "Config" suffix from properties.
 */
public class ConfigPropertyNamingStrategy extends PropertyNamingStrategy.KebabCaseStrategy {
  private static final String CONFIG_SUFFIX = "Config";

  @Override
  public String translate(String input) {
    if (isConfigProperty(input)) {
      input = toSimpleProperty(input);
    }
    if (isKebabCase(input)) {
      input = toCamelCase(input);
    }
    return super.translate(input);
  }

  private static boolean isConfigProperty(String propertyName) {
    return propertyName.endsWith(CONFIG_SUFFIX);
  }

  private static String toSimpleProperty(String propertyName) {
    return propertyName.substring(0, propertyName.length() - CONFIG_SUFFIX.length());
  }

  private static boolean isKebabCase(String propertyName) {
    return propertyName.contains("-");
  }

  private static String toCamelCase(String propertyName) {
    String[] words = propertyName.split("-+");
    StringBuilder nameBuilder = new StringBuilder(propertyName.length());
    for (String word : words) {
      if (nameBuilder.length() == 0) {
        nameBuilder.append(word);
      } else {
        nameBuilder.append(word.substring(0, 1).toUpperCase());
        nameBuilder.append(word.substring(1));
      }
    }
    return nameBuilder.toString();
  }
}
