package net.kuujo.copycat.protocol;

/**
 * Install response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallResponse extends Response {
  private long term;

  public InstallResponse(long term) {
    super(Status.OK);
    this.term = term;
  }

  public InstallResponse(Throwable t) {
    super(Status.ERROR, t);
  }

  public InstallResponse(String error) {
    super(Status.ERROR, error);
  }

  /**
   * Returns the response term.
   *
   * @return The response term.
   */
  public long term() {
    return term;
  }

}
