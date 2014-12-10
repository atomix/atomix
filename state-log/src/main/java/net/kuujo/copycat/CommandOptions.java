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
package net.kuujo.copycat;

/**
 * State log command options.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandOptions {
  private boolean consistent = true;
  private boolean readOnly = false;

  /**
   * Sets whether the command requires a consistency check.
   *
   * @param consistent Whether the command requires a consistency check.
   */
  public void setConsistent(boolean consistent) {
    this.consistent = consistent;
  }

  /**
   * Returns whether the command requires a consistency check.
   *
   * @return Whether the command requires a consistency check.
   */
  public boolean isConsistent() {
    return consistent;
  }

  /**
   * Sets whether the command requires a consistency check, returning the command options for method chaining.
   *
   * @param consistent Whether the command requires a consistency check.
   * @return The command options.
   */
  public CommandOptions withConsistent(boolean consistent) {
    this.consistent = consistent;
    return this;
  }

  /**
   * Sets whether the command is a read-only command.
   *
   * @param readOnly Whether the command is a read-only command.
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Returns whether the command is a read-only command.
   *
   * @return Whether the command is a read-only command.
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Sets whether the command is a read-only command, returning the command options for method chaining.
   *
   * @param readOnly Whether the command is a read-only command.
   * @return The command options.
   */
  public CommandOptions withReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

}
