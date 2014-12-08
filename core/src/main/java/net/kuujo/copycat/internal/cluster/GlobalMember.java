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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.ManagedMember;

import java.util.concurrent.CompletableFuture;

/**
 * Internal member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class GlobalMember implements ManagedMember {
  private final String uri;
  private State state;

  protected GlobalMember(String uri) {
    this.uri = uri;
  }

  @Override
  public String uri() {
    return uri;
  }

  @Override
  public State state() {
    return state;
  }

  public void state(State state) {
    this.state = state;
  }

  abstract <T, U> CompletableFuture<U> send(String topic, int address, T message);

}
