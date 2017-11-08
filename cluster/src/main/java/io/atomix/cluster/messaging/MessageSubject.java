/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster.messaging;

import com.google.common.base.Preconditions;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of a message subject.
 * Cluster messages have associated subjects that dictate how they get handled
 * on the receiving side.
 */
public final class MessageSubject {

  private final String name;

  public MessageSubject(String name) {
    this.name = Preconditions.checkNotNull(name);
  }

  /**
   * Returns the subject name.
   *
   * @return the message subject name
   */
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MessageSubject that = (MessageSubject) obj;
    return Objects.equals(this.name, that.name);
  }

  // for serializer
  protected MessageSubject() {
    this.name = "";
  }
}
