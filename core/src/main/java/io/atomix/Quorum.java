/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix;

/**
 * Quorum configuration constants.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum Quorum {

  /**
   * Indicates that the seed members should represent the quorum size.
   */
  SEED(0),

  /**
   * Indicates that all members of the cluster should participate in the quorum.
   */
  ALL(-1);

  private final int size;

  Quorum(int size) {
    this.size = size;
  }

  /**
   * Returns the quorum size.
   *
   * @return The quorum size.
   */
  public int size() {
    return size;
  }

}
