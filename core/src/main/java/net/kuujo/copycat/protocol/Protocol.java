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
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.util.Managed;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Copycat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Protocol implements Managed<Protocol> {
  protected final Set<EventListener<Event>> listeners = new CopyOnWriteArraySet<>();
  protected String topic;
  protected Cluster cluster;

  /**
   * Sets the protocol cluster.
   *
   * @param cluster The protocol cluster.
   */
  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns the protocol cluster.
   *
   * @return The protocol cluster.
   */
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * Sets the protocol topic.
   *
   * @param topic The protocol topic.
   */
  public void setTopic(String topic) {
    this.topic = topic;
  }

  /**
   * Returns the protocol topic.
   *
   * @return The protocol topic.
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Adds an event listener to the protocol.
   *
   * @param listener The event listener to add.
   * @return The protocol.
   */
  @SuppressWarnings("unchecked")
  public Protocol addListener(EventListener<? extends Event> listener) {
    listeners.add((EventListener<Event>) listener);
    return this;
  }

  /**
   * Removes an event listener from the protocol.
   *
   * @param listener The event listener to remove.
   * @return The protocol.
   */
  public Protocol removeListener(EventListener<? extends Event> listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Submits a keyless command to the protocol.
   *
   * @param entry The command entry.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer entry) {
    return submit(null, entry, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Submits a keyless command to the protocol with the default consistency level.
   *
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer entry, Persistence persistence) {
    return submit(null, entry, persistence, Consistency.DEFAULT);
  }

  /**
   * Submits a keyless command to the protocol with the default persistence level.
   *
   * @param entry The command entry.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer entry, Consistency consistency) {
    return submit(null, entry, Persistence.DEFAULT, consistency);
  }

  /**
   * Submits a command to the protocol with the default persistence and consistency levels.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer key, Buffer entry) {
    return submit(key, entry, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default consistency level.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Persistence persistence) {
    return submit(key, entry, persistence, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default persistence level.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Consistency consistency) {
    return submit(key, entry, Persistence.DEFAULT, consistency);
  }

  /**
   * Submits a command to the protocol.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  public abstract CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Persistence persistence, Consistency consistency);

  /**
   * Registers a protocol commit handler.
   *
   * @param handler The protocol commit handler.
   * @return The protocol.
   */
  public abstract Protocol commitHandler(CommitHandler handler);

  /**
   * Protocol builder.
   */
  public static abstract class Builder implements net.kuujo.copycat.Builder<Protocol> {
  }

}
