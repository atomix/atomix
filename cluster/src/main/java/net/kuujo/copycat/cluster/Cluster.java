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

import net.kuujo.copycat.AbstractResource;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.raft.Raft;

/**
 * Cluster resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Cluster extends AbstractResource {
  private final Listeners<Member> joinListeners = new Listeners<>();
  private final Listeners<Member> leaveListeners = new Listeners<>();

  public Cluster(Raft protocol) {
    super(protocol);
  }

  /**
   * Listens for a member joining the cluster.
   *
   * @param listener The member listener.
   * @return The listener context.
   */
  public ListenerContext<Member> onMemberJoined(Listener<Member> listener) {
    return joinListeners.add(listener);
  }

  /**
   * Listens for a member leaving the cluster.
   *
   * @param listener The member listener.
   * @return The listener context.
   */
  public ListenerContext<Member> onMemberLeft(Listener<Member> listener) {
    return leaveListeners.add(listener);
  }

}
