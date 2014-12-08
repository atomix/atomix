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
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandOptions {
  private boolean consistent = true;
  private boolean persistent = true;

  public void setConsistent(boolean consistent) {
    this.consistent = consistent;
  }

  public boolean isConsistent() {
    return consistent;
  }

  public CommandOptions withConsistent(boolean consistent) {
    this.consistent = consistent;
    return this;
  }

  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public CommandOptions withPersistent(boolean persistent) {
    this.persistent = persistent;
    return this;
  }

}
