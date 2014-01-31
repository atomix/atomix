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
 * A state factory.
 * 
 * @author Jordan Halterman
 */
public class StateFactory {

  /**
   * Creates a new start state.
   * 
   * @return A new start state.
   */
  public Start createStart() {
    return new Start();
  }

  /**
   * Creates a new follower state.
   * 
   * @return A new follower state.
   */
  public Follower createFollower() {
    return new Follower();
  }

  /**
   * Creates a new candidate state.
   * 
   * @return A new candidate state.
   */
  public Candidate createCandidate() {
    return new Candidate();
  }

  /**
   * Creates a new leader state.
   * 
   * @return A new leader state.
   */
  public Leader createLeader() {
    return new Leader();
  }

}
