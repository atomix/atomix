/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.atomix.core.tree;

import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Unique key for nodes in the {@link AtomicDocumentTree}.
 */
public class DocumentPath implements Comparable<DocumentPath> {
  private static final String ROOT_ELEMENT = "";

  /**
   * Creates a new {@code DocumentPath} from a period delimited path string.
   *
   * @param path path string
   * @return {@code DocumentPath} instance
   */
  public static DocumentPath from(String path) {
    if (path.equals(ROOT_PATH)) {
      return ROOT;
    }
    return new DocumentPath(Arrays.asList(path.split(PATH_SEPARATOR)));
  }

  /**
   * Creates a new {@code DocumentPath} from a list of path elements.
   *
   * @param elements path elements
   * @return {@code DocumentPath} instance
   */
  public static DocumentPath from(String... elements) {
    return from(Arrays.asList(elements));
  }

  /**
   * Creates a new root {@code DocumentPath} from a list of path elements.
   *
   * @param elements path elements
   * @return {@code DocumentPath} instance
   */
  public static DocumentPath from(List<String> elements) {
    List<String> copy = new ArrayList<>();
    copy.add(ROOT_ELEMENT);
    copy.addAll(elements);
    return new DocumentPath(copy);
  }

  /**
   * Creates a new root {@code DocumentPath} from a list of path elements.
   *
   * @param elements path elements
   * @param child    child element
   * @return {@code DocumentPath} instance
   */
  public static DocumentPath from(List<String> elements, String child) {
    List<String> copy = new ArrayList<>();
    copy.add(ROOT_ELEMENT);
    copy.addAll(elements);
    copy.add(child);
    return new DocumentPath(copy);
  }

  /**
   * Default path separator.
   */
  private static final String PATH_SEPARATOR = "/";

  /**
   * Root document path.
   */
  private static final String ROOT_PATH = "/";

  /**
   * Root document tree path.
   */
  public static final DocumentPath ROOT = new DocumentPath(ImmutableList.of(ROOT_ELEMENT));

  private final List<String> pathElements;

  /**
   * Private utility constructor for internal generation of partial paths only.
   *
   * @param pathElements list of path elements
   */
  private DocumentPath(List<String> pathElements) {
    checkNotNull(pathElements);
    this.pathElements = ImmutableList.copyOf(pathElements);
  }

  /**
   * Constructs a new document path.
   * <p>
   * New paths must contain at least one name and string names may NOT contain any period characters.
   * If one field is {@code null} that field will be ignored.
   *
   * @param nodeName   the name of the last level of this path
   * @param parentPath the path representing the parent leading up to this
   *                   node, in the case of the root this should be {@code null}
   * @throws IllegalDocumentNameException if both parameters are null or name contains an illegal character ('.')
   */
  public DocumentPath(String nodeName, DocumentPath parentPath) {
    checkNotNull(nodeName, "Node name cannot be null");
    if (nodeName.contains(PATH_SEPARATOR)) {
      throw new IllegalDocumentNameException("'" + PATH_SEPARATOR + "' are not allowed in names.");
    }

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    if (parentPath != null) {
      builder.addAll(parentPath.pathElements());
    }
    builder.add(nodeName);

    pathElements = builder.build();
    if (pathElements.isEmpty()) {
      throw new IllegalDocumentNameException("A document path must contain at least one non-null element.");
    }
  }

  /**
   * Returns the relative  path to the given node.
   *
   * @return relative  path to the given node.
   */
  public DocumentPath childPath() {
    if (pathElements.size() <= 1) {
      return null;
    }
    return new DocumentPath(this.pathElements.subList(pathElements.size() - 1, pathElements.size()));
  }

  /**
   * Returns a path for the parent of this node.
   *
   * @return parent node path. If this path is for the root, returns {@code null}.
   */
  public DocumentPath parent() {
    if (pathElements.size() <= 1) {
      return null;
    }
    return new DocumentPath(this.pathElements.subList(0, pathElements.size() - 1));
  }

  /**
   * Returns the list of path elements representing this path in correct
   * order.
   *
   * @return a list of elements that make up this path
   */
  public List<String> pathElements() {
    return ImmutableList.copyOf(pathElements);
  }

  /**
   * Returns if the specified path belongs to a direct ancestor of the node pointed at by this path.
   * <p>
   * Example: {@code root.a} is a direct ancestor of {@code r.a.b.c}; while {@code r.a.x} is not.
   *
   * @param other other path
   * @return {@code true} is yes; {@code false} otherwise.
   */
  public boolean isAncestorOf(DocumentPath other) {
    return !other.equals(this) && other.toString().startsWith(toString());
  }

  /**
   * Returns if the specified path is belongs to a subtree rooted this path.
   * <p>
   * Example: {@code root.a.b} and {@code root.a.b.c.d.e} are descendants of {@code r.a.b};
   * while {@code r.a.x.c} is not.
   *
   * @param other other path
   * @return {@code true} is yes; {@code false} otherwise.
   */
  public boolean isDescendentOf(DocumentPath other) {
    return other.equals(this) || other.isAncestorOf(this);
  }

  /**
   * Returns the path that points to the least common ancestor of the specified
   * collection of paths.
   *
   * @param paths collection of path
   * @return path to least common ancestor
   */
  public static DocumentPath leastCommonAncestor(Collection<DocumentPath> paths) {
    if (paths.isEmpty()) {
      return null;
    }
    return DocumentPath.from(StringUtils.getCommonPrefix(paths.stream()
        .map(DocumentPath::toString)
        .toArray(String[]::new)));
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathElements);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DocumentPath) {
      DocumentPath that = (DocumentPath) obj;
      return this.pathElements.equals(that.pathElements);
    }
    return false;
  }

  @Override
  public String toString() {
    if (pathElements.equals(ROOT.pathElements)) {
      return ROOT_PATH;
    }

    StringBuilder stringBuilder = new StringBuilder();
    Iterator<String> iter = pathElements.iterator();
    while (iter.hasNext()) {
      stringBuilder.append(iter.next());
      if (iter.hasNext()) {
        stringBuilder.append(PATH_SEPARATOR);
      }
    }
    return stringBuilder.toString();
  }

  @Override
  public int compareTo(DocumentPath that) {
    return Comparators.lexicographical(Comparator.<String>naturalOrder())
        .compare(this.pathElements, that.pathElements);
  }
}
