package net.kuujo.copycat.protocol;

/**
 * Install request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallRequest extends Request<InstallResponse> {
  private long term;
  private String leader;
  private long snapshotIndex;
  private long snapshotTerm;
  private byte[] data;
  private boolean complete;

  public InstallRequest(long term, String leader, long snapshotIndex, long snapshotTerm, byte[] data, boolean complete) {
    this.term = term;
    this.leader = leader;
    this.snapshotIndex = snapshotIndex;
    this.snapshotTerm = snapshotTerm;
    this.data = data;
    this.complete = complete;
  }

  /**
   * Returns the leader's current term.
   *
   * @return The leader's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the leader sending the snapshot.
   *
   * @return The identifier of the leader sending the snapshot.
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns the snapshot's index in the log.
   *
   * @return The index of the snapshot entry in the log.
   */
  public long snapshotIndex() {
    return snapshotIndex;
  }

  /**
   * Returns the snapshot's term in the log.
   *
   * @return The term of the snapshot entry in the log.
   */
  public long snapshotTerm() {
    return snapshotTerm;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  /**
   * Returns a boolean indicating whether the snapshot is complete.
   *
   * @return Indicates whether all the data for the snapshot has been sent.
   */
  public boolean complete() {
    return complete;
  }

  /**
   * Responds to the request.
   *
   * @param term The current term.
   */
  public void respond(long term) {
    super.respond(new InstallResponse(term));
  }

  /**
   * Responds to the request.
   *
   * @param t A response exception.
   */
  public void respond(Throwable t) {
    super.respond(new InstallResponse(t));
  }

  /**
   * Responds to the request with an error.
   *
   * @param error The error message.
   */
  public void respond(String error) {
    super.respond(new InstallResponse(error));
  }

}
