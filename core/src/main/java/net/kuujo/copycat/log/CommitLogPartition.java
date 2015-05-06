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
package net.kuujo.copycat.log;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBufferPool;
import net.kuujo.copycat.io.util.ReferencePool;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Commit log partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommitLogPartition<KEY, VALUE> extends CommitLog<KEY, VALUE> {
  private final String name;
  private final Cluster cluster;
  private final ProtocolInstance protocol;
  private final ReferencePool<Buffer> bufferPool = new HeapBufferPool();
  private RawCommitHandler handler;

  CommitLogPartition(String name, Cluster cluster, Protocol protocol, ExecutionContext context) {
    this.name = name;
    this.cluster = cluster;
    this.protocol = protocol.createInstance(name, cluster, context);
    this.protocol.setHandler(this::commit);
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the partition cluster.
   *
   * @return The partition cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

  @Override
  protected CommitLog<KEY, VALUE> filter(ProtocolFilter filter) {
    protocol.setFilter(filter);
    return this;
  }

  @Override
  public <RESULT> CommitLog<KEY, VALUE> handler(CommitHandler<KEY, VALUE, RESULT> handler) {
    return handler(wrapHandler(handler));
  }

  /**
   * Wraps a commit handler in serialization handling.
   */
  private RawCommitHandler wrapHandler(CommitHandler<KEY, VALUE, ?> handler) {
    return (index, key, entry, result) -> cluster.serializer().writeObject(handler.commit(index, cluster.serializer().readObject(key), cluster.serializer().readObject(entry)), result);
  }

  @Override
  protected CommitLog<KEY, VALUE> handler(RawCommitHandler handler) {
    this.handler = handler;
    return this;
  }

  @Override
  public <RESULT> CompletableFuture<RESULT> commit(KEY key, VALUE value, Persistence persistence, Consistency consistency) {
    Buffer keyBuffer = cluster.serializer().writeObject(key, bufferPool.acquire()).flip();
    Buffer entryBuffer = cluster.serializer().writeObject(value, bufferPool.acquire()).flip();
    return commit(keyBuffer, entryBuffer, persistence, consistency).thenApply((result) -> {
      keyBuffer.close();
      entryBuffer.close();
      return cluster.serializer().readObject(result);
    });
  }

  @Override
  protected CompletableFuture<Buffer> commit(Buffer key, Buffer entry, Persistence persistence, Consistency consistency) {
    key.acquire();
    entry.acquire();
    return protocol.submit(key, entry, persistence, consistency).thenApply((result) -> {
      key.release();
      entry.release();
      return cluster.serializer().readObject(result);
    });
  }

  /**
   * Handles an entry commit.
   */
  private Buffer commit(long index, Buffer key, Buffer entry, Buffer result) {
    if (handler != null) {
      return handler.commit(index, key, entry, result);
    }
    return cluster.serializer().writeObject(null, result);
  }

  /**
   * Opens the partition.
   *
   * @return A completable future to be completed once the partition is opened.
   */
  public CompletableFuture<CommitLog<KEY, VALUE>> open() {
    return protocol.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return protocol.isOpen();
  }

  /**
   * Closes the partition.
   *
   * @return A completable future to be completed once the partition is closed.
   */
  public CompletableFuture<Void> close() {
    return protocol.close();
  }

  @Override
  public boolean isClosed() {
    return protocol.isClosed();
  }

}
