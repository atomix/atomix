package net.kuujo.copycat.cluster;

import java.io.Serializable;
import java.net.URI;

import net.kuujo.copycat.internal.util.Assert;

public class ClusterMember implements Serializable {
  private static final long serialVersionUID = 5814184209234325889L;

  private final String id;
  private final URI endpoint;

  public ClusterMember() {
    id = null;
    this.endpoint = null;
  }

  /**
   * @throws NullPointerException if {@code endpoint} is null
   */
  public ClusterMember(String endpoint) {
    this.endpoint = URI.create(Assert.isNotNull(endpoint, "endpoint"));
    this.id = memberIdFor(this.endpoint);
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  public String id() {
    return id;
  }

  public URI endpoint() {
    return endpoint;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ClusterMember && ((ClusterMember) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    int hashCode = 47;
    hashCode = 37 * hashCode + id.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("ClusterMember[%s]", id);
  }
  
  private static String memberIdFor(URI uri) {
    StringBuilder sb = new StringBuilder();
    if (uri.getScheme() != null)
      sb.append(uri.getScheme()).append("://");
    sb.append(uri.getPath());
    if (uri.getPort() != -1)
      sb.append(':').append(uri.getPort());
    return sb.toString();
  }
}