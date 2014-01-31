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
package net.kuujo.copycat.state;

/**
 * A Raft state type.
 * 
 * @author Jordan Halterman
 */
public enum StateType {
  START("start"), FOLLOWER("follower"), CANDIDATE("candidate"), LEADER("leader");

  private final String name;

  private StateType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return getName();
  }

  public static StateType fromName(String name) {
    switch (name) {
      case "start":
        return START;
      case "follower":
        return FOLLOWER;
      case "candidate":
        return CANDIDATE;
      case "leader":
        return LEADER;
      default:
        throw new IllegalArgumentException("Invalid state name " + name);
    }
  }

}
