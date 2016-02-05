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
 * limitations under the License
 */
package io.atomix;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.QualifiedProperties;
import io.atomix.copycat.server.storage.StorageLevel;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Properties;

/**
 * Replica properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ReplicaProperties extends AtomixProperties {
  public static final String TRANSPORT = "node.transport";
  public static final String ADDRESS = "node.address";
  public static final String CLIENT_ADDRESS = "node.clientAddress";
  public static final String SERVER_ADDRESS = "node.serverAddress";
  public static final String QUORUM_HINT = "cluster.quorumHint";
  public static final String BACKUP_COUNT = "cluster.backupCount";
  public static final String ELECTION_TIMEOUT = "cluster.electionTimeout";
  public static final String HEARTBEAT_INTERVAL = "cluster.heartbeatInterval";
  public static final String SESSION_TIMEOUT = "cluster.sessionTimeout";
  public static final String STORAGE_DIRECTORY = "storage.directory";
  public static final String STORAGE_LEVEL = "storage.level";
  public static final String MAX_SEGMENT_SIZE = "storage.maxSegmentSize";
  public static final String MAX_ENTRIES_PER_SEGMENT = "storage.maxEntriesPerSegment";
  public static final String MAX_SNAPSHOT_SIZE = "storage.compaction.maxSnapshotSize";
  public static final String RETAIN_STALE_SNAPSHOTS = "storage.compaction.retainSnapshots";
  public static final String COMPACTION_THREADS = "storage.compaction.threads";
  public static final String MINOR_COMPACTION_INTERVAL = "storage.compaction.minor";
  public static final String MAJOR_COMPACTION_INTERVAL = "storage.compaction.major";
  public static final String COMPACTION_THRESHOLD = "storage.compaction.threshold";

  private static final String DEFAULT_TRANSPORT = "io.atomix.catalyst.transport.NettyTransport";
  private static final int DEFAULT_QUORUM_HINT = -1;
  private static final int DEFAULT_BACKUP_COUNT = 0;
  private static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofMillis(500);
  private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(250);
  private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(5);
  private static final File DEFAULT_STORAGE_DIRECTORY = new File(System.getProperty("user.dir"));
  private static final StorageLevel DEFAULT_STORAGE_LEVEL = StorageLevel.DISK;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = 1024 * 1024;
  private static final int DEFAULT_MAX_SNAPSHOT_SIZE = 1024 * 1024 * 32;
  private static final boolean DEFAULT_RETAIN_STALE_SNAPSHOTS = false;
  private static final int DEFAULT_COMPACTION_THREADS = Runtime.getRuntime().availableProcessors() / 2;
  private static final Duration DEFAULT_MINOR_COMPACTION_INTERVAL = Duration.ofMinutes(1);
  private static final Duration DEFAULT_MAJOR_COMPACTION_INTERVAL = Duration.ofHours(1);
  private static final double DEFAULT_COMPACTION_THRESHOLD = 0.5;

  public ReplicaProperties(Properties properties) {
    super(properties);
  }

  /**
   * Returns the replica transport.
   *
   * @return The replica transport.
   */
  public Transport transport() {
    String transportClass = reader.getString(TRANSPORT, DEFAULT_TRANSPORT);
    try {
      return (Transport) Class.forName(transportClass).getConstructor(Properties.class).newInstance(new QualifiedProperties(reader.properties(), TRANSPORT));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("unknown transport class: " + transportClass, e);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new ConfigurationException("failed to instantiate transport", e);
    }
  }

  /**
   * Returns the replica client address.
   *
   * @return The replica client address.
   */
  public Address clientAddress() {
    return parseAddress(reader.getString(CLIENT_ADDRESS, reader.getString(SERVER_ADDRESS, reader.getString(ADDRESS))));
  }

  /**
   * Returns the replica server address.
   *
   * @return The replica server address.
   */
  public Address serverAddress() {
    return parseAddress(reader.getString(SERVER_ADDRESS, reader.getString(ADDRESS)));
  }

  /**
   * Returns the quorum hint.
   *
   * @return The quorum hint.
   */
  public int quorumHint() {
    return reader.getInteger(QUORUM_HINT, DEFAULT_QUORUM_HINT);
  }

  /**
   * Returns the backup count.
   *
   * @return The backup count.
   */
  public int backupCount() {
    return reader.getInteger(BACKUP_COUNT, DEFAULT_BACKUP_COUNT);
  }

  /**
   * Returns the election timeout.
   *
   * @return The election timeout.
   */
  public Duration electionTimeout() {
    return reader.getDuration(ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT);
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval.
   */
  public Duration heartbeatInterval() {
    return reader.getDuration(HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL);
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public Duration sessionTimeout() {
    return reader.getDuration(SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Returns the storage directory.
   *
   * @return The storage directory.
   */
  public File storageDirectory() {
    return reader.getFile(STORAGE_DIRECTORY, DEFAULT_STORAGE_DIRECTORY);
  }

  /**
   * Returns the storage level.
   *
   * @return The storage level.
   */
  public StorageLevel storageLevel() {
    return reader.getEnum(STORAGE_LEVEL, StorageLevel.class, DEFAULT_STORAGE_LEVEL);
  }

  /**
   * Returns the maximum segment size in bytes.
   *
   * @return The maximum segment size in bytes.
   */
  public int maxSegmentSize() {
    return reader.getInteger(MAX_SEGMENT_SIZE, DEFAULT_MAX_SEGMENT_SIZE);
  }

  /**
   * Returns the maximum number of entries per segment.
   *
   * @return The maximum number of entries per segment.
   */
  public int maxEntriesPerSegment() {
    return reader.getInteger(MAX_ENTRIES_PER_SEGMENT, DEFAULT_MAX_ENTRIES_PER_SEGMENT);
  }

  /**
   * Returns the maximum snapshot size in bytes.
   *
   * @return The maximum snapshot size in bytes.
   */
  public int maxSnapshotSize() {
    return reader.getInteger(MAX_SNAPSHOT_SIZE, DEFAULT_MAX_SNAPSHOT_SIZE);
  }

  /**
   * Returns a boolean indicating whether to retain stale snapshots.
   *
   * @return A boolean indicating whether to retain stale snapshots.
   */
  public boolean retainStaleSnapshots() {
    return reader.getBoolean(RETAIN_STALE_SNAPSHOTS, DEFAULT_RETAIN_STALE_SNAPSHOTS);
  }

  /**
   * Returns the number of storage compaction threads.
   *
   * @return The number of storage compaction threads.
   */
  public int compactionThreads() {
    return reader.getInteger(COMPACTION_THREADS, DEFAULT_COMPACTION_THREADS);
  }

  /**
   * Returns the minor compaction interval.
   *
   * @return The minor compaction interval.
   */
  public Duration minorCompactionInterval() {
    return reader.getDuration(MINOR_COMPACTION_INTERVAL, DEFAULT_MINOR_COMPACTION_INTERVAL);
  }

  /**
   * Returns the major compaction interval.
   *
   * @return The major compaction interval.
   */
  public Duration majorCompactionInterval() {
    return reader.getDuration(MAJOR_COMPACTION_INTERVAL, DEFAULT_MAJOR_COMPACTION_INTERVAL);
  }

  /**
   * Returns the number of compaction threads.
   *
   * @return The number of compaction threads.
   */
  public double compactionThreshold() {
    return reader.getDouble(COMPACTION_THRESHOLD, DEFAULT_COMPACTION_THRESHOLD);
  }

}
