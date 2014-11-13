package ca.thejayvm.hex.routing;

import org.junit.Test;
import static org.junit.Assert.*;
import static servlet_mock.HttpMock.*;

/**
 * Created by jason on 14-11-11.
 */
public class RoutingConfigTest {
    @Test
    public void staticRoutes() {
        RoutingConfig config = new RoutingConfig();
        config.addRoute("/people", (q, r) -> q.setAttribute("Boring", "Boring"));
        RouteHandler handler = config.getRouteHandler("/people");
        assertNotNull("Should retrieve a static path", handler);
        GET("/people", handler::handleRequest)
                .andThen((q, r) -> assertEquals("Boring", q.getAttribute("Boring")));
    }

    @Test
    public void dynamicRouteWithNamedParam() {
        RoutingConfig config = new RoutingConfig();
        config.addRoute("/posts/:id", (q, r) -> q.setAttribute("post_id", q.getAttribute("id")));
        RouteHandler handler = config.getRouteHandler("/posts/7");
        assertNotNull("Should retrieve paramed path", handler);
        GET("/posts/7", handler::handleRequest)
                .andThen((q, r) -> assertEquals(7, q.getAttribute("post_id")));
    }
}
