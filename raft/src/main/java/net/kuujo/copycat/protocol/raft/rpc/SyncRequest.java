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
package net.kuujo.copycat.protocol.raft.rpc;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.NativeBuffer;
import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.protocol.raft.RaftMember;
import net.kuujo.copycat.protocol.raft.RaftMemberPool;
import net.kuujo.copycat.protocol.raft.storage.RaftEntry;
import net.kuujo.copycat.protocol.raft.storage.RaftEntryPool;

import java.util.*;

/**
 * Protocol sync request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncRequest extends AbstractRequest<SyncRequest> {
  private static final ThreadLocal<Builder> builder = new ThreadLocal<Builder>() {
    @Override
    protected Builder initialValue() {
      return new Builder();
    }
  };
  private static final ThreadLocal<RaftEntryPool> entryPool = new ThreadLocal<RaftEntryPool>() {
    @Override
    protected RaftEntryPool initialValue() {
      return new RaftEntryPool();
    }
  };
  private static final ThreadLocal<RaftMemberPool> memberPool = new ThreadLocal<RaftMemberPool>() {
    @Override
    protected RaftMemberPool initialValue() {
      return new RaftMemberPool();
    }
  };

  /**
   * Returns a new sync request builder.
   *
   * @return A new sync request builder.
   */
  public static Builder builder() {
    return builder.get().reset();
  }

  /**
   * Returns a sync request builder for an existing request.
   *
   * @param request The request to build.
   * @return The sync request builder.
   */
  public static Builder builder(SyncRequest request) {
    return builder.get().reset(request);
  }

  private long term;
  private int leader;
  private long logIndex;
  private List<RaftEntry> entries = new ArrayList<>(128);
  private Collection<RaftMember> members = new ArrayList<>(128);

  public SyncRequest(ReferenceManager<SyncRequest> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Type type() {
    return Type.SYNC;
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  public int leader() {
    return leader;
  }

  /**
   * Returns the last known log index.
   *
   * @return The last known log index.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  public List<RaftEntry> entries() {
    return entries;
  }

  /**
   * Returns the currently known membership.
   *
   * @return The currently known membership.
   */
  public Collection<RaftMember> members() {
    return members;
  }

  @Override
  public void readObject(Buffer buffer) {
    term = buffer.readLong();
    leader = buffer.readInt();
    logIndex = buffer.readLong();
    int entriesSize = buffer.readInt();
    if (entriesSize < 0)
      throw new SerializationException("invalid entries size: " + entriesSize);

    RaftEntryPool epool = entryPool.get();

    entries.clear();
    for (int i = 0; i < entriesSize; i++) {
      RaftEntry entry = epool.acquire(buffer.readLong());
      entry.readObject(buffer);
      entries.add(entry);
    }

    int membersSize = buffer.readInt();
    if (membersSize < 0)
      throw new SerializationException("invalid members size: " + membersSize);

    RaftMemberPool mpool = memberPool.get();

    members.clear();
    for (int i = 0; i < membersSize; i++) {
      RaftMember member = mpool.acquire();
      member.readObject(buffer);
      members.add(member);
    }
  }

  @Override
  public void writeObject(Buffer buffer) {
    buffer.writeLong(term)
      .writeInt(leader)
      .writeLong(logIndex);

    buffer.writeInt(entries.size());
    for (RaftEntry entry : entries) {
      entry.writeObject(buffer.writeLong(entry.index()));
    }

    buffer.writeInt(members.size());
    for (RaftMember member : members) {
      member.writeObject(buffer);
    }
  }

  @Override
  public void close() {
    entries.forEach(RaftEntry::release);
    members.forEach(RaftMember::release);
    super.close();
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, leader, entries);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncRequest) {
      SyncRequest request = (SyncRequest) object;
      return request.term == term
        && request.leader == leader
        && request.logIndex == logIndex
        && request.entries.equals(entries)
        && request.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%s, logIndex=%s, entries=[%d]]", getClass().getSimpleName(), term, leader, logIndex, entries.size());
  }

  /**
   * Sync request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, SyncRequest> {
    private final Buffer buffer = NativeBuffer.allocate(1024);

    private Builder() {
      super(SyncRequest::new);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The sync request builder.
     */
    public Builder withTerm(long term) {
      if (term <= 0)
        throw new IllegalArgumentException("term must be positive");
      request.term = term;
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The sync request builder.
     */
    public Builder withLeader(int leader) {
      request.leader = leader;
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    public Builder withEntries(RaftEntry... entries) {
      return withEntries(Arrays.asList(entries));
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    public Builder withEntries(List<RaftEntry> entries) {
      if (entries == null)
        throw new NullPointerException("entries cannot be null");
      request.entries = entries;
      return this;
    }

    /**
     * Sets the request log index.
     *
     * @param index The request log index.
     * @return The request builder.
     */
    public Builder withLogIndex(long index) {
      if (index < 0)
        throw new IllegalArgumentException("log index must be positive");
      request.logIndex = index;
      return this;
    }

    /**
     * Sets the request membership.
     *
     * @param members The request membership.
     * @return The sync request builder.
     */
    public Builder withMembers(Collection<RaftMember> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      request.members = members;
      return this;
    }

    @Override
    public SyncRequest build() {
      super.build();
      if (request.term <= 0)
        throw new IllegalArgumentException("term must be positive");
      if (request.logIndex < 0)
        throw new IllegalArgumentException("log index must be positive");
      if (request.entries == null)
        throw new NullPointerException("entries cannot be null");
      if (request.members == null)
        throw new NullPointerException("members cannot be null");

      buffer.clear();
      request.writeObject(buffer);
      buffer.flip();
      request.readObject(buffer);
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
