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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.Event;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Copycat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractProtocol implements Protocol {
  protected final Set<EventListener<Event>> listeners = new CopyOnWriteArraySet<>();
  protected String topic;
  protected Cluster cluster;
  protected ExecutionContext context;

  @Override
  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  public void setTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  @Override
  public void setContext(ExecutionContext context) {
    this.context = context;
  }

  @Override
  public ExecutionContext getContext() {
    return context;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Protocol addListener(EventListener<? extends Event> listener) {
    listeners.add((EventListener<Event>) listener);
    return this;
  }

  @Override
  public Protocol removeListener(EventListener<? extends Event> listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Protocol builder.
   */
  public static abstract class Builder implements Protocol.Builder {
  }

}
