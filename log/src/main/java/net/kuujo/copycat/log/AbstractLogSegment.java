package net.kuujo.copycat.log;

import java.util.Comparator;

/**
 * Base LogSegment implementation.
 * 
 * @author Jonathan Halterman
 */
public abstract class AbstractLogSegment extends AbstractLoggable implements LogSegment, Comparator<AbstractLogSegment> {
  protected final long id;
  protected long firstIndex;

  protected AbstractLogSegment(long id, long firstIndex) {
    this.id = id;
    this.firstIndex = firstIndex;
  }

  @Override
  public int compare(AbstractLogSegment a, AbstractLogSegment b) {
    return Long.compare(a.firstIndex, b.firstIndex);
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public long index() {
    return firstIndex;
  }

  @Override
  public String toString() {
    return String.format("%s..%s", firstIndex(), lastIndex());
  }

}
