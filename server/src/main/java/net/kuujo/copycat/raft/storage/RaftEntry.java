package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.storage.Entry;
import net.kuujo.copycat.util.ReferenceManager;

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

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(term);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    term = buffer.readLong();
  }

}
