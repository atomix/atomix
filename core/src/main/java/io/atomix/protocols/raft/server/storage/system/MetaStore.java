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
package io.atomix.protocols.raft.server.storage.system;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.server.storage.Storage;
import io.atomix.protocols.raft.server.storage.StorageLevel;
import io.atomix.util.Assert;
import io.atomix.util.buffer.Buffer;
import io.atomix.util.buffer.FileBuffer;
import io.atomix.util.buffer.HeapBuffer;
import io.atomix.util.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Manages persistence of server configurations.
 * <p>
 * The server metastore is responsible for persisting server configurations according to the configured
 * {@link Storage#level() storage level}. Each server persists their current {@link #loadTerm() term}
 * and last {@link #loadVote() vote} as is dictated by the Raft consensus algorithm. Additionally, the
 * metastore is responsible for storing the last know server {@link Configuration}, including cluster
 * membership.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MetaStore implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetaStore.class);
  private final Storage storage;
  private final Serializer serializer;
  private final FileBuffer metadataBuffer;
  private final Buffer configurationBuffer;

  public MetaStore(String name, Storage storage, Serializer serializer) {
    this.storage = Assert.notNull(storage, "storage");
    this.serializer = Assert.notNull(serializer, "serializer");

    Assert.notNull(storage.directory(), "null storage directory");
    if (!(storage.directory().isDirectory() || storage.directory().mkdirs())) {
      throw new IllegalArgumentException(String.format("Can't create storage directory [%s].", storage.directory()));
    }

    // Note that for raft safety, irrespective of the storage level, <term, vote> metadata is always persisted on disk.
    File metaFile = new File(storage.directory(), String.format("%s.meta", name));
    FileBuffer tmpMetadataBuffer = FileBuffer.allocate(metaFile, 12);

    if (storage.level() == StorageLevel.MEMORY) {
      configurationBuffer = HeapBuffer.allocate(32);
    } else {
      File confFile = new File(storage.directory(), String.format("%s.conf", name));
      configurationBuffer = FileBuffer.allocate(confFile, 32);

      //Note: backward compatibility with pre 1.2.6 release. This can be removed in later releases.
      Configuration configuration = loadConfiguration();
      if (configuration == null) {
        configuration = loadConfigurationFromMetadataBuffer(tmpMetadataBuffer);
        if (configuration != null) {
          storeConfiguration(configuration);
          tmpMetadataBuffer = deleteConfigurationFromMetadataBuffer(metaFile, tmpMetadataBuffer);
        }
      }
    }

    metadataBuffer = tmpMetadataBuffer;
  }

  //Note: used in backward compatibility code with pre 1.2.6 release. This can be removed in later releases.
  private Configuration loadConfigurationFromMetadataBuffer(Buffer buffer) {
    if (buffer.position(12).readByte() == 1) {
      return new Configuration(
        buffer.readLong(),
        buffer.readLong(),
        buffer.readLong(),
        serializer.readObject(buffer)
      );
    }
    return null;
  }

  //Note: used in backward compatibility code with pre 1.2.6 release. This can be removed in later releases.
  private FileBuffer deleteConfigurationFromMetadataBuffer(File metaFile, FileBuffer buffer)
  {
    long term = buffer.readLong(0);
    int vote = buffer.readInt(8);

    buffer.close();
    try {
      Files.delete(metaFile.toPath());
    } catch (IOException ex) {
      throw new RuntimeException(String.format("Failed to delete [%s].", metaFile), ex);
    }

    FileBuffer truncatedBuffer = FileBuffer.allocate(metaFile, 12);
    truncatedBuffer.writeLong(0, term).flush();
    truncatedBuffer.writeInt(8, vote).flush();

    return truncatedBuffer;
  }

  /**
   * Returns the metastore serializer.
   *
   * @return The metastore serializer.
   */
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Stores the current server term.
   *
   * @param term The current server term.
   * @return The metastore.
   */
  public synchronized MetaStore storeTerm(long term) {
    LOGGER.trace("Store term {}", term);
    metadataBuffer.writeLong(0, term).flush();
    return this;
  }

  /**
   * Loads the stored server term.
   *
   * @return The stored server term.
   */
  public synchronized long loadTerm() {
    return metadataBuffer.readLong(0);
  }

  /**
   * Stores the last voted server.
   *
   * @param vote The server vote.
   * @return The metastore.
   */
  public synchronized MetaStore storeVote(NodeId vote) {
    LOGGER.trace("Store vote {}", vote);
    metadataBuffer.writeInt(8, vote).flush();
    return this;
  }

  /**
   * Loads the last vote for the server.
   *
   * @return The last vote for the server.
   */
  public synchronized NodeId loadVote() {
    return metadataBuffer.readInt(8);
  }

  /**
   * Stores the current cluster configuration.
   *
   * @param configuration The current cluster configuration.
   * @return The metastore.
   */
  public synchronized MetaStore storeConfiguration(Configuration configuration) {
    LOGGER.trace("Store configuration {}", configuration);
    serializer.writeObject(configuration.members(), configurationBuffer.position(0)
      .writeByte(1)
      .writeLong(configuration.index())
      .writeLong(configuration.term())
      .writeLong(configuration.time()));
    configurationBuffer.flush();
    return this;
  }

  /**
   * Loads the current cluster configuration.
   *
   * @return The current cluster configuration.
   */
  public synchronized Configuration loadConfiguration() {
    if (configurationBuffer.position(0).readByte() == 1) {
      return new Configuration(
          configurationBuffer.readLong(),
          configurationBuffer.readLong(),
          configurationBuffer.readLong(),
          serializer.readObject(configurationBuffer)
      );
    }
    return null;
  }

  @Override
  public synchronized void close() {
    metadataBuffer.close();
    configurationBuffer.close();
  }

  @Override
  public String toString() {
    if (configurationBuffer instanceof FileBuffer) {
      return String.format(
        "%s[%s,%s]",
        getClass().getSimpleName(),
        metadataBuffer.file(),
        ((FileBuffer) configurationBuffer).file()
      );
    } else {
      return String.format(
        "%s[%s]",
        getClass().getSimpleName(),
        metadataBuffer.file()
      );
    }
  }

}
