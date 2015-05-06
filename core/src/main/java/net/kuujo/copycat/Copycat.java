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
package net.kuujo.copycat;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.log.*;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceFactory;
import net.kuujo.copycat.util.Managed;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Copycat :-P
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat implements Managed<Copycat> {
  private final String name;
  private final CopycatCommitLog log;
  private final Map<Integer, ResourceInfo> resourceInfo = new ConcurrentHashMap<>();
  private final Map<String, Resource> resources = new ConcurrentHashMap<>();
  private final Map<Integer, Resource> contexts = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  private Copycat(String name, CommitLog log) {
    this.name = name;
    this.log = new CopycatCommitLog(log);
    this.log.handler(name, this::commit);
  }

  /**
   * Commits a resource.
   */
  @SuppressWarnings("unchecked")
  private Object commit(long index, Object key, Object entry) {
    if (key == null || entry == null)
      return null;

    String resource = key.toString();
    int resourceId = log.calculateHash(resource);
    ResourceInfo info = resourceInfo.get(resourceId);
    ResourceCommit commit = (ResourceCommit) entry;

    if (commit.action.equals("open")) {
      if (info != null) {
        info.permits++;
      } else {
        info = new ResourceInfo();
        info.index = index;
        info.permits = 1;
        resourceInfo.put(resourceId, info);
        try {
          contexts.put(resourceId, (Resource) commit.factory.createResource(new ResourceCommitLog(resource, log)).open().get());
        } catch (InterruptedException | ExecutionException e) {
          throw new IllegalStateException("failed to open resource", e);
        }
      }
    } else if (commit.action.equals("close")) {
      if (info != null) {
        info.permits--;
        if (info.permits == 0) {
          resourceInfo.remove(resourceId);
          Resource context = contexts.remove(resourceId);
          if (context != null) {
            try {
              context.close().get();
            } catch (InterruptedException | ExecutionException e) {
              throw new IllegalStateException("failed to close resource", e);
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Returns a named resource.
   *
   * @param name The resource name.
   * @return The named resource.
   */
  @SuppressWarnings("unchecked")
  public <RESOURCE extends Resource<?>> RESOURCE getResource(String name, ResourceFactory<RESOURCE> factory) {
    return (RESOURCE) resources.computeIfAbsent(name, n -> factory.createResource(new CopycatResourceLog(name, factory, log)));
  }

  /**
   * Opens the given resource.
   */
  @SuppressWarnings("unchecked")
  protected CompletableFuture<Void> openResource(String name, ResourceFactory factory) {
    return log.commit(this.name, name, new ResourceCommit("open", factory), Persistence.PERSISTENT, Consistency.STRICT).thenRun(() -> {});
  }

  /**
   * Closes the given resource.
   */
  @SuppressWarnings("unchecked")
  protected CompletableFuture<Void> closeResource(String name) {
    return log.commit(this.name, name, new ResourceCommit("close"), Persistence.PERSISTENT, Consistency.STRICT).thenRun(() -> {});
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Copycat> open() {
    return log.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return log.isOpen();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> close() {
    return log.close();
  }

  @Override
  public boolean isClosed() {
    return log.isClosed();
  }

  /**
   * Copycat commit log.
   */
  private class CopycatCommitLog extends SharedCommitLog {
    @SuppressWarnings("unchecked")
    private CopycatCommitLog(CommitLog log) {
      super(log);
      filter(this::filter);
    }

    @Override
    protected int calculateHash(String value) {
      return super.calculateHash(value);
    }

    /**
     * Filters entries from old resources out of the log.
     */
    private boolean filter(long index, Buffer key, Buffer entry) {
      int hash = entry.readInt();
      ResourceInfo info = resourceInfo.get(hash);
      return info == null || index < info.index;
    }
  }

  /**
   * Copycat resource log.
   */
  private class CopycatResourceLog extends ResourceCommitLog {
    private final ResourceFactory factory;

    @SuppressWarnings("unchecked")
    private CopycatResourceLog(String name, ResourceFactory factory, SharedCommitLog log) {
      super(name, log);
      this.factory = factory;
    }

    @Override
    public CommitLog handler(CommitHandler handler) {
      // Prevent commit handlers from being registered.
      return this;
    }

    @Override
    protected CommitLog handler(RawCommitHandler handler) {
      // Prevent commit handlers from being registered.
      return this;
    }

    @Override
    public CompletableFuture<CommitLog> open() {
      return openResource(name(), factory).thenCompose(v -> super.open());
    }

    @Override
    public CompletableFuture<Void> close() {
      return closeResource(name()).thenCompose(v -> super.close());
    }
  }

  /**
   * Resource commit.
   */
  protected static class ResourceCommit implements Serializable {
    private final String action;
    private final ResourceFactory factory;

    public ResourceCommit(String action) {
      this(action, null);
    }

    private ResourceCommit(String action, ResourceFactory factory) {
      this.action = action;
      this.factory = factory;
    }
  }

  /**
   * Resource info.
   */
  private static class ResourceInfo {
    private long index;
    private long permits;
  }

  /**
   * Copycat builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Copycat> {
    private static int i;
    private String name;
    private CommitLog log;

    /**
     * Sets the Copycat name instance name.
     *
     * @param name The Copycat instance name.
     * @return The Copycat builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the Copycat instance log.
     *
     * @param log The instance commit log.
     * @return The Copycat builder.
     */
    public Builder withLog(CommitLog log) {
      this.log = log;
      return this;
    }

    @Override
    public Copycat build() {
      return new Copycat(name != null ? name : String.format("copycat-%d", ++i), log);
    }
  }

}
