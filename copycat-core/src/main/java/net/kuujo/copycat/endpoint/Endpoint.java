package net.kuujo.copycat.endpoint;

import net.kuujo.copycat.util.AsyncCallback;

/**
 * CopyCat endpoint.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Endpoint {

  Endpoint start(AsyncCallback<Void> callback);

  void stop(AsyncCallback<Void> callback);

}
