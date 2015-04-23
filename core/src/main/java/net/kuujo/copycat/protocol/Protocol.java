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
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Copycat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Protocol implements Managed<Protocol> {
  protected final Set<EventListener<Event>> listeners = new ConcurrentSkipListSet<>();
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
  @SuppressWarnings("unchecked")
  public Protocol removeListener(EventListener<? extends Event> listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Submits a read to the protocol.
   *
   * @param entry The read entry.
   * @return A completable future to be completed with the read result.
   */
  public CompletableFuture<Buffer> read(Buffer entry) {
    return read(null, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a read to the protocol.
   *
   * @param key The read key.
   * @param entry The read entry.
   * @return A completable future to be completed with the read result.
   */
  public CompletableFuture<Buffer> read(Buffer key, Buffer entry) {
    return read(key, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a read to the protocol.
   *
   * @param key The read key.
   * @param entry The read entry.
   * @param consistency The read consistency.
   * @return A completable future to be completed with the read result.
   */
  public abstract CompletableFuture<Buffer> read(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Submits a write to the protocol.
   *
   * @param entry The write entry.
   * @return A completable future to be completed with the write result.
   */
  public CompletableFuture<Buffer> write(Buffer entry) {
    return write(null, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a write to the protocol.
   *
   * @param key The write key.
   * @param entry The write entry.
   * @return A completable future to be completed with the write result.
   */
  public CompletableFuture<Buffer> write(Buffer key, Buffer entry) {
    return write(key, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a write to the protocol.
   *
   * @param key The write key.
   * @param entry The write entry.
   * @param consistency The write consistency.
   * @return A completable future to be completed with the write result.
   */
  public abstract CompletableFuture<Buffer> write(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Submits a delete to the protocol.
   *
   * @param entry The delete entry.
   * @return A completable future to be completed with the delete result.
   */
  public CompletableFuture<Buffer> delete(Buffer entry) {
    return delete(null, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a delete to the protocol.
   *
   * @param key The delete key.
   * @param entry The delete entry.
   * @return A completable future to be completed with the delete result.
   */
  public CompletableFuture<Buffer> delete(Buffer key, Buffer entry) {
    return delete(key, entry, Consistency.DEFAULT);
  }

  /**
   * Submits a delete to the protocol.
   *
   * @param key The delete key.
   * @param entry The delete entry.
   * @param consistency The delete consistency.
   * @return A completable future to be completed with the delete result.
   */
  public abstract CompletableFuture<Buffer> delete(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Registers a protocol commit handler.
   *
   * @param handler The protocol commit handler.
   * @return The protocol.
   */
  public abstract Protocol commit(CommitHandler handler);

  /**
   * Protocol builder.
   */
  public static abstract class Builder implements net.kuujo.copycat.Builder<Protocol> {
  }

}
