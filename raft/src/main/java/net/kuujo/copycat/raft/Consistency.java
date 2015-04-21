/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft;

/**
 * Read consistency.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum Consistency {

  /**
   * Indicates that consistency is not required to be guaranteed during reads.
   */
  WEAK(-1, "weak"),

  /**
   * Indicates that consistency should be attempted during reads but is not required.
   */
  DEFAULT(0, "default"),

  /**
   * Indicates that consistency should be guaranteed for reads.
   */
  STRONG(1, "strong");

  /**
   * Returns the consistency level for a consistency identifier.
   *
   * @param id The consistency identifier.
   * @return The consistency constant.
   */
  public static Consistency forId(int id) {
    switch (id) {
      case -1:
        return WEAK;
      case 0:
        return DEFAULT;
      case 1:
        return STRONG;
      default:
        throw new IllegalArgumentException("invalid consistency identifier " + id);
    }
  }

  /**
   * Returns the consistency for the given consistency name.
   *
   * @param name The consistency name.
   * @return The consistency constant.
   */
  public static Consistency forName(String name) {
    switch (name) {
      case "weak":
        return WEAK;
      case "default":
        return DEFAULT;
      case "strong":
        return STRONG;
      default:
        throw new IllegalArgumentException("invalid consistency name " + name);
    }
  }

  private final byte id;
  private final String name;

  private Consistency(int id, String name) {
    this.id = (byte) id;
    this.name = name;
  }

  /**
   * Returns the consistency identifier.
   *
   * @return The consistency identifier.
   */
  public int id() {
    return id;
  }

  @Override
  public String toString() {
    return name;
  }

}
