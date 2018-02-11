/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.storage.journal.index;

/**
 * Journal index.
 */
public interface JournalIndex {

  /**
   * Adds an entry for the given index at the given position.
   *
   * @param index the index for which to add the entry
   * @param position the position of the given index
   */
  void index(long index, int position);

  /**
   * Looks up the position of the given index.
   *
   * @param index the index to lookup
   * @return the position of the given index or a lesser index
   */
  Position lookup(long index);

  /**
   * Truncates the index to the given index.
   *
   * @param index the index to which to truncate the index
   */
  void truncate(long index);

}
