package com.orangerhymelabs.orangedb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.restexpress.RestExpress;
import org.restexpress.exception.BadRequestException;
import org.restexpress.exception.ConflictException;
import org.restexpress.exception.NotFoundException;
import org.restexpress.pipeline.SimpleConsoleLogMessageObserver;
import org.restexpress.plugin.hyperexpress.HyperExpressPlugin;
import org.restexpress.plugin.hyperexpress.Linkable;
import org.restexpress.plugin.version.VersionPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.orangerhymelabs.orangedb.serialization.SerializationProvider;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;
import com.strategicgains.restexpress.plugin.metrics.MetricsPlugin;
import com.strategicgains.syntaxe.ValidationException;

public class Main
{
    private static final String SERVICE_NAME = "OrangeDB REST API";
    private static final Logger LOG = LoggerFactory.getLogger(SERVICE_NAME);

    public static void main(String[] args) throws Exception
    {
        RestExpress server = initializeServer(args);
        server.awaitShutdown();
    }

    public static RestExpress initializeServer(String[] args) throws IOException
    {
        RestExpress.setSerializationProvider(new SerializationProvider());
        Identifiers.UUID.useShortUUID(true);

        Configuration config = Configuration.load(args, Configuration.class);
        RestExpress server = new RestExpress()
                .setName(config.getProjectName(SERVICE_NAME))
                .setBaseUrl(config.getBaseUrl())
                .setExecutorThreadCount(config.getExecutorThreadPoolSize())
                .addMessageObserver(new SimpleConsoleLogMessageObserver());

        new VersionPlugin(config.getProjectVersion())
                .register(server);

        Routes.define(config, server);
        Relationships.define(server);
        configurePlugins(config, server);
        mapExceptions(server);
        server.bind(config.getPort());
        return server;
    }

    private static void configurePlugins(Configuration config, RestExpress server)
    {
        configureMetrics(config, server);

        new HyperExpressPlugin(Linkable.class)
           .register(server);
    }

    private static void configureMetrics(Configuration config, RestExpress server)
    {
        MetricsConfig mc = config.getMetricsConfig();

        if (mc.isEnabled())
        {
            MetricRegistry registry = new MetricRegistry();
            new MetricsPlugin(registry)
                    .register(server);

            if (mc.isGraphiteEnabled())
            {
                final Graphite graphite = new Graphite(new InetSocketAddress(mc.getGraphiteHost(), mc.getGraphitePort()));
                final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                        .prefixedWith(mc.getPrefix())
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .filter(MetricFilter.ALL)
                        .build(graphite);
                reporter.start(mc.getPublishSeconds(), TimeUnit.SECONDS);
            } else
            {
                LOG.warn("*** Graphite Metrics Publishing is Disabled ***");
            }
        } else
        {
            LOG.warn("*** Metrics Generation is Disabled ***");
        }
    }

    private static void mapExceptions(RestExpress server)
    {
        server
                .mapException(ItemNotFoundException.class, NotFoundException.class)
                .mapException(DuplicateItemException.class, ConflictException.class)
                .mapException(ValidationException.class, BadRequestException.class)
                .mapException(InvalidObjectIdException.class, BadRequestException.class);
    }
}
