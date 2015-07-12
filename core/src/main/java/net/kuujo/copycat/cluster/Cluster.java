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
package net.kuujo.copycat.cluster;

import java.util.Collection;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns a cluster topic.
   *
   * @param name The topic name.
   * @param <T> The topic message type.
   * @return The topic.
   */
  <T> Topic<T> topic(String name);

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member.
   */
  Member member(int id);

  /**
   * Returns an immutable collection of all cluster members.
   *
   * @return A collection of all cluster members.
   */
  Collection<Member> members();

}
