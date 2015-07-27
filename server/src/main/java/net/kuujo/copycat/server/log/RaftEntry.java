package net.kuujo.copycat.server.log;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.alleycat.util.ReferenceManager;
import net.kuujo.copycat.log.Entry;

/**
 * Raft entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftEntry<T extends RaftEntry<T>> extends Entry<T> {
  private long term;

  protected RaftEntry() {
    super();
  }

  protected RaftEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the entry term.
   *
   * @param term The entry term.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setTerm(long term) {
    this.term = term;
    return (T) this;
  }

  /**
   * Returns the entry size.
   *
   * @return The entry size.
   */
  public int size() {
    return 8;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeLong(term);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    term = buffer.readLong();
  }

}
