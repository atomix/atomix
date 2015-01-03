/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

/**
 * Read consistency.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum Consistency {

  /**
   * Indicates that consistency is not required to be guaranteed during reads.
   */
  NONE("none"),

  /**
   * Indicates that consistency should be attempted during reads but is not required.
   */
  DEFAULT("default"),

  /**
   * Indicates that consistency should be guaranteed for reads.
   */
  FULL("full");

  public static Consistency parse(String name) {
    switch (name) {
      case "none":
        return NONE;
      case "default":
        return DEFAULT;
      case "full":
        return FULL;
      default:
        throw new IllegalArgumentException("Invalid consistency name " + name);
    }
  }

  private final String name;

  private Consistency(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

}
