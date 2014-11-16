package hex.routing;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * Created by jason on 14-11-11.
 */
@FunctionalInterface
public interface RouteHandler {
    public void handleRequest(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException;
}
