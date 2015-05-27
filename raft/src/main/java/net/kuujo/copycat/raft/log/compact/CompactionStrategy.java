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
package net.kuujo.copycat.raft.log.compact;

import net.kuujo.copycat.raft.log.RaftEntryFilter;
import net.kuujo.copycat.raft.log.SegmentManager;

/**
 * Log compaction strategy.
 * <p>
 * The compaction strategy handles the logic behind compacting logs after deduplication.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CompactionStrategy {

  /**
   * Compacts the given segments.
   *
   * @param filter The filter with which to compact segments.
   * @param segments The segments to compact.
   */
  void compact(RaftEntryFilter filter, SegmentManager segments);

}
