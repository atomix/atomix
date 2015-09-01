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
package net.kuujo.copycat.io.storage;

import net.kuujo.copycat.io.PooledDirectAllocator;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.Assert;

import java.io.File;

/**
 * Immutable log configuration/factory.
 * <p>
 * This class provides a factory for {@link Log} objects. {@code Storage} objects are immutable and
 * can be created only via the {@link net.kuujo.copycat.io.storage.Storage.Builder}. To create a new
 * {@code Storage.Builder}, use the static {@link #builder()} factory method:
 * <pre>
 *   {@code
 *     Storage storage = Storage.builder()
 *       .withDirectory(new File("logs"))
 *       .withPersistenceLevel(PersistenceLevel.DISK)
 *       .build();
 *   }
 * </pre>
 * Users can also configure a number of options related to how {@link Log logs} are constructed and managed.
 * Most notable of the configuration options is the number of {@link #cleanerThreads()}, which specifies the
 * number of background threads to use to clean log {@link Segment segments}. The parallelism of the log
 * compaction algorithm will be limited by the number of {@link #cleanerThreads()}.
 *
 * @see Log
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Storage {
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 8;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = (int) (Math.pow(2, 31) - 1) / 8 - 16;
  private static final int DEFAULT_CLEANER_THREADS = Runtime.getRuntime().availableProcessors() / 2;

  private Serializer serializer = new Serializer(new PooledDirectAllocator());
  private File directory = new File(DEFAULT_DIRECTORY);
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
  private int cleanerThreads = DEFAULT_CLEANER_THREADS;

  public Storage() {
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(String directory) {
    this(new File(Assert.notNull(directory, "directory")));
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(File directory) {
    this.directory = Assert.notNull(directory, "directory");
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(Serializer serializer) {
    this.serializer = Assert.notNull(serializer, "serializer");
  }
  
  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(String directory, Serializer serializer) {
    this(new File(Assert.notNull(directory, "directory")), serializer);
  }

  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(File directory, Serializer serializer) {
    this.directory = Assert.notNull(directory, "directory");
    this.serializer = Assert.notNull(serializer, "serializer");
  }

  /**
   * Returns a new storage builder.
   *
   * @return A new storage builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the storage serializer.
   *
   * @return The storage serializer.
   */
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Returns the storage directory.
   *
   * @return The storage directory.
   */
  public File directory() {
    return directory;
  }

  /**
   * Returns the maximum storage entry size.
   *
   * @return The maximum entry size in bytes.
   */
  public int maxEntrySize() {
    return maxEntrySize;
  }

  /**
   * Returns the maximum storage segment size.
   *
   * @return The maximum segment size in bytes.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns the maximum number of entries per segment.
   *
   * @return The maximum number of entries per segment.
   */
  public int maxEntriesPerSegment() {
    return maxEntriesPerSegment;
  }

  /**
   * Returns the number of log cleaner threads.
   *
   * @return The number of log cleaner threads.
   */
  public int cleanerThreads() {
    return cleanerThreads;
  }

  /**
   * Opens the underlying log.
   *
   * @return The opened log.
   */
  public Log open() {
    return new Log(this);
  }

  @Override
  public String toString() {
    return String.format("%s[directory=%s]", getClass().getSimpleName(), directory);
  }

  /**
   * Storage builder.
   */
  public static class Builder extends net.kuujo.copycat.util.Builder<Storage> {
    private final Storage storage = new Storage();

    private Builder() {
    }

    /**
     * Sets the log entry serializer.
     *
     * @param serializer The log entry serializer.
     * @return The log builder.
     * @throws NullPointerException If the serializer is {@code null}
     */
    public Builder withSerializer(Serializer serializer) {
      storage.serializer = Assert.notNull(serializer, "serializer");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The log builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      return withDirectory(new File(Assert.notNull(directory, "directory")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The log builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      storage.directory = Assert.notNull(directory, "directory");
      return this;
    }

    /**
     * Sets the maximum entry count, returning the builder for method chaining.
     * <p>
     * The maximum entry count will be used to place an upper limit on the count of log segments.
     *
     * @param maxEntrySize The maximum entry count.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive or {@code maxEntrySize} is not 
     * less than the max segment size.
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      Assert.arg(maxEntrySize > 0, "maximum entry size must be positive");
      Assert.argNot(maxEntrySize > storage.maxSegmentSize, "maximum entry size must be less than maxSegmentSize");
      storage.maxEntrySize = maxEntrySize;
      return this;
    }

    /**
     * Sets the maximum segment count, returning the builder for method chaining.
     *
     * @param maxSegmentSize The maximum segment count.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive or {@code maxSegmentSize} 
     * is not greater than the maxEntrySize
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      Assert.arg(maxSegmentSize > 0, "maxSegmentSize must be positive");
      Assert.argNot(maxSegmentSize < storage.maxEntrySize, "maximum segment size must be greater than maxEntrySize");
      storage.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code maxEntriesPerSegment} not greater than the default max entries per 
     * segment
     */
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      Assert.argNot(maxEntriesPerSegment > DEFAULT_MAX_ENTRIES_PER_SEGMENT,
          "max entries per segment cannot be greater than " + DEFAULT_MAX_ENTRIES_PER_SEGMENT);
      storage.maxEntriesPerSegment = maxEntriesPerSegment;
      return this;
    }

    /**
     * Sets the number of log cleaner threads.
     *
     * @param cleanerThreads The number of log cleaner threads.
     * @return The log builder.
     * @throws IllegalArgumentException if {@code cleanerThreads} is not positive
     */
    public Builder withCleanerThreads(int cleanerThreads) {
      storage.cleanerThreads = Assert.arg(cleanerThreads, cleanerThreads > 0, "cleanerThreads must be positive");
      return this;
    }

    @Override
    public Storage build() {
      return storage;
    }
  }

}
