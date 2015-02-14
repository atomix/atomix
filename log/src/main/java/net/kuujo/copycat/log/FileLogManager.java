/*
 * Copyright 2014 the original author or authors.
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * File log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLogManager extends AbstractLogManager {
  final FileLog config;
  final File base;

  FileLogManager(String name, FileLog config) {
    super(config);
    this.config = config.copy();
    this.base = new File(config.getDirectory(), name);
  }

  @Override
  protected Collection<LogSegment> loadSegments() {
    Map<Long, LogSegment> segments = new HashMap<>();
    base.getAbsoluteFile().getParentFile().mkdirs();
    for (File file : config.getDirectory().listFiles(File::isFile)) {
      if (file.getName().startsWith(base.getName() + "-") && file.getName().endsWith(".metadata")) {
        try {
          long id = Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-') + 1), file.getName().lastIndexOf('.')).longValue();
          if (!segments.containsKey(id)) {
            // Open the metadata file, determine the segment's first index, and create a log segment.
            try (RandomAccessFile metaFile = new RandomAccessFile(file, "r")) {
              long firstIndex = metaFile.readLong();
              segments.put(id, new FileLogSegment(this, id, firstIndex));
            }
          }
        } catch (IOException | NumberFormatException e) {
          throw new LogException(e);
        }
      }
    }
    return segments.values();
  }

  @Override
  protected LogSegment createSegment(long segmentId, long firstIndex) {
    return new FileLogSegment(this, segmentId, firstIndex);
  }

}
