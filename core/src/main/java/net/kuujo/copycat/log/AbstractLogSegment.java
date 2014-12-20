package net.kuujo.copycat.log;

/**
 * Base LogSegment implementation.
 * 
 * @author Jonathan Halterman
 */
public abstract class AbstractLogSegment extends AbstractLoggable implements LogSegment {
  @Override
  public int compare(LogSegment a, LogSegment b) {
    return Long.compare(a.segment(), b.segment());
  }
  
  @Override
  public String toString() {
    return String.valueOf(segment());
  }
}
