package com.sf.hadoop;

import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.util.ServicePlugin;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Optional;

public abstract class ReconfigurableServicePlugin implements ServicePlugin {

    public static final Log LOGGER = LogFactory.getLog(ReconfigurableServicePlugin.class);

    public static final String RECONFIGURATION_KEY = "reconfig";

    public static final String TEMPLATE_NAME = "reconfigurable-plugin.html";

    public static class ReloadableServlet extends HttpServlet {

        protected ReconfigurableServicePlugin plugin(HttpServletRequest request) {
            String plugin_key = request.getServletPath().substring(1);
            return (ReconfigurableServicePlugin) this.getServletContext()
                    .getAttribute(contextPlugin(plugin_key));
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
            boolean reload = Optional.ofNullable(request.getParameter("reload"))
                    .map(Boolean::parseBoolean)
                    .orElse(false);

            try {
                ReconfigurableServicePlugin reconfigurable = plugin(request);
                if (reload) {
                    reconfigurable.doReconfigurate(request);
                }

                // check response style
                boolean json = Optional.ofNullable(request.getParameter("format"))
                        .orElse("html")
                        .equalsIgnoreCase("json");

                String content = reconfigurable.render();
                if (json) {
                    response.setContentType("application/json");
                    response.getWriter().print(content);
                    return;
                }

                response.setContentType("text/html");
                response.getWriter().print(
                        loadTemplate(request)
                                .replaceAll("__JSON_TEMPLATE__", content)
                                .replaceAll("__RECONFIG_PATH__", request.getServletPath())
                );
            } catch (Throwable throwable) {
                LOGGER.error("fail to precess reload for plugin:" + request.getContextPath(), throwable);
                response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "plugin reload not accepted");
            }

            return;
        }
    }

    protected Object service;
    protected HttpServer2 http;

    protected static String contextPlugin(String name) {
        return RECONFIGURATION_KEY + "/" + name;
    }

    protected static String loadTemplate(HttpServletRequest request) {
        String plugin_name = request.getServletPath().substring(1);
        return Optional.ofNullable(Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(
                        String.format(
                                "%s-%s",
                                plugin_name,
                                TEMPLATE_NAME
                        )
                ))
                .map((stream) -> {
                    try (ReadableByteChannel channel = Channels.newChannel(stream)) {
                        ByteBuffer content = ByteBuffer.allocate(4096);
                        while (channel.read(content) != -1) {
                            if (!content.hasRemaining()) {
                                ByteBuffer copy = ByteBuffer.allocate(content.capacity() + 1024);
                                content.flip();
                                copy.put(content);
                                content = copy;
                            }
                        }

                        // to read mode
                        content.flip();

                        return new String(content.array(), content.position(), content.limit());
                    } catch (Throwable e) {
                        throw new UncheckedIOException(new IOException("fail to load resource:" + TEMPLATE_NAME, e));
                    }
                })
                .orElse("");
    }

    @Override
    public void start(Object service) {
        this.service = service;
        try {
            this.http = findHttpServer();
            LOGGER.info("plugin find httpserver:" + http);
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to find http server for:" + this, throwable);
        }

        String name = name();
        String path = "/" + name;

        HttpServer2 http = http();
        http.addServlet(name, path, ReloadableServlet.class);
        http.setAttribute(contextPlugin(name), this);

        LOGGER.info("plug reconfig servlet with name:" + name
                + " path:" + path
                + " attribute:" + this
                + "(" + contextPlugin(name) + "=" + this + ")"
        );
    }

    @Override
    public void stop() {
        LOGGER.info("no action for stop");
    }

    @Override
    public void close() {
        this.stop();
    }

    protected abstract void doReconfigurate(HttpServletRequest request) throws Throwable;

    protected abstract String name();

    protected abstract HttpServer2 findHttpServer() throws Throwable;

    protected abstract String render();

    protected HttpServer2 http() {
        return http;
    }

    protected Object service() {
        return service;
    }
}
