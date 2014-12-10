/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

/**
 * Action options.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ActionOptions {
  private boolean consistent = true;
  private boolean persistent = true;

  /**
   * Sets whether the action should be executed with consistency.
   *
   * @param consistent Whether to execute the action with consistency.
   */
  public void setConsistent(boolean consistent) {
    this.consistent = consistent;
  }

  /**
   * Returns whether to execute the action with consistency.
   *
   * @return Whether to execute the action with consistency.
   */
  public boolean isConsistent() {
    return consistent;
  }

  /**
   * Sets whether the action should be executed with consistency, returning the options for method chaining.
   *
   * @param consistent Whether to execute the action with consistency.
   * @return The action options.
   */
  public ActionOptions withConsistent(boolean consistent) {
    this.consistent = consistent;
    return this;
  }

  /**
   * Sets whether the action should be persisted to the log.
   *
   * @param persistent Whether to persist the action to the log.
   */
  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }

  /**
   * Returns whether to persist the action to the log.
   *
   * @return Whether to persist the action to the log.
   */
  public boolean isPersistent() {
    return persistent;
  }

  /**
   * Sets whether the action should be persisted to the log, returning the options for method chaining.
   *
   * @param persistent Whether to persist the action to the log.
   * @return The action options.
   */
  public ActionOptions withPersistent(boolean persistent) {
    this.persistent = persistent;
    return this;
  }

}
