package hex.dev;

import hex.Application;
import hex.routing.*;

import javax.servlet.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by jason on 14-11-21.
 */
public class DevRoutingFilter implements Filter, RoutingConfig {
    private RoutingConfig config;

    private RoutingFilter filter = new RoutingFilter();

    private Supplier<Stream<String>> sourcePaths;

    private String outPath;

    private URL[] getSourcePathURLs() throws MalformedURLException {
        return new URL[] {
                new File(applicationRoot, outPath).toURI().toURL()
        };
    }

    private String applicationRoot;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        applicationRoot = filterConfig.getInitParameter(Application.ROOT);
        Properties applicationProperties = new Properties();
        File applicationRoot = new File(this.applicationRoot, Application.CONFIG);
        try(InputStream propStream = new FileInputStream(applicationRoot)) {
            applicationProperties.load(propStream);
            sourcePaths = () -> Stream.of(applicationProperties.getProperty("src").split(",")).map(String::trim);
            outPath = applicationProperties.getProperty("out");
            filterConfig.getServletContext().setAttribute(Routing.CONFIG, this);
            filter.init(filterConfig);
        } catch (IOException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        try (URLClassLoader requestClassLoader = new URLClassLoader(getSourcePathURLs(), this.getClass().getClassLoader())) {
            new Compiler(applicationRoot).compile(sourcePaths, outPath);
            config = Application.initializeRoutes(requestClassLoader);
            filter.doFilter(servletRequest, servletResponse, filterChain);
        }  finally {
            config = null;
        }
    }

    @Override
    public void destroy() {
        filter.destroy();
    }

    @Override
    public boolean hasRoute(String path) {
        return config.hasRoute(path);
    }

    @Override
    public boolean hasRoute(String method, String path) {
        return config.hasRoute(method, path);
    }

    @Override
    public boolean hasRoute(HttpMethod method, String path) {
        return config.hasRoute(method, path);
    }

    @Override
    public RouteHandler getRouteHandler(String path) {
        return config.getRouteHandler(path);
    }

    @Override
    public RouteHandler getRouteHandler(String method, String path) {
        return config.getRouteHandler(method, path);
    }

    @Override
    public RouteHandler getRouteHandler(HttpMethod method, String path) {
        return config.getRouteHandler(method, path);
    }
}
