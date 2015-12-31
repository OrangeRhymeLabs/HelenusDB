package com.orangerhymelabs.helenusdb.rest;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.jar.Manifest;

import org.restexpress.RestExpress;
import org.restexpress.common.exception.ConfigurationException;
import org.restexpress.util.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orangerhymelabs.helenusdb.cassandra.CassandraConfig;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenusdb.cassandra.document.DocumentRepositoryFactory;
import com.orangerhymelabs.helenusdb.cassandra.document.DocumentRepositoryFactoryImpl;
import com.orangerhymelabs.helenusdb.cassandra.document.DocumentService;
import com.orangerhymelabs.helenusdb.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenusdb.cassandra.table.TableRepository;
import com.orangerhymelabs.helenusdb.cassandra.table.TableService;
import com.orangerhymelabs.helenusdb.rest.database.DatabaseController;
import com.orangerhymelabs.helenusdb.rest.document.DocumentController;
import com.orangerhymelabs.helenusdb.rest.table.TableController;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;

public class Configuration
extends Environment
{
    private static final String DEFAULT_EXECUTOR_THREAD_POOL_SIZE = "20";

    private static final String PORT_PROPERTY = "port";
    private static final String BASE_URL_PROPERTY = "base.url";
    private static final String EXECUTOR_THREAD_POOL_SIZE = "executor.threadPool.size";

    private int port;
    private String baseUrl;
    private int executorThreadPoolSize;
    private MetricsConfig metricsSettings;
    private Manifest manifest;

    private DatabaseController databaseController;
    private TableController tableController;
    private DocumentController documentController;

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    @Override
    public void fillValues(Properties p)
    {
        this.port = Integer.parseInt(p.getProperty(PORT_PROPERTY, String.valueOf(RestExpress.DEFAULT_PORT)));
        this.baseUrl = p.getProperty(BASE_URL_PROPERTY, "http://localhost:" + String.valueOf(port));
        this.executorThreadPoolSize = Integer.parseInt(p.getProperty(EXECUTOR_THREAD_POOL_SIZE, DEFAULT_EXECUTOR_THREAD_POOL_SIZE));
        this.metricsSettings = new MetricsConfig(p);
        CassandraConfig dbConfig = new CassandraConfig(p);
        initialize(dbConfig);
        loadManifest();
    }

    private void initialize(CassandraConfig dbConfig)
    {
        DatabaseRepository databaseRepository = new DatabaseRepository(dbConfig.getSession(), dbConfig.getKeyspace());
        TableRepository tableRepository = new TableRepository(dbConfig.getSession(), dbConfig.getKeyspace());
        IndexRepository indexRepository = new IndexRepository(dbConfig.getSession(), dbConfig.getKeyspace());
        DocumentRepositoryFactory documentRepositoryFactory = new DocumentRepositoryFactoryImpl(dbConfig.getSession(), dbConfig.getKeyspace(), indexRepository);

        DatabaseService databaseService = new DatabaseService(databaseRepository);
        TableService tableService = new TableService(databaseRepository, tableRepository);
        DocumentService documentService = new DocumentService(tableService, documentRepositoryFactory);

        databaseController = new DatabaseController(databaseService);
        tableController = new TableController(tableService);
        documentController = new DocumentController(documentService);

//        EventBus bus = new LocalEventBusBuilder()
//                .subscribe(new TableDeleteHandler(dbConfig.getSession()))
//                .subscribe(new DatabaseDeletedHandler(dbConfig.getSession()))
//                .build();
//        DomainEvents.addBus("local", bus);
    }

    public int getPort()
    {
        return port;
    }

    public String getBaseUrl()
    {
        return baseUrl;
    }

    public int getExecutorThreadPoolSize()
    {
        return executorThreadPoolSize;
    }

    public MetricsConfig getMetricsConfig()
    {
        return metricsSettings;
    }

    public DatabaseController getDatabaseController()
    {
        return databaseController;
    }

    public TableController getTableController()
    {
        return tableController;
    }

    public DocumentController getDocumentController()
    {
        return documentController;
    }

    public String getProjectName(String defaultName)
    {
        if (hasManifest())
        {
            String name = manifest.getMainAttributes().getValue("Project-Name");

            if (name != null)
            {
                return name;
            }
        }

        return defaultName;
    }

    public String getProjectVersion()
    {
        if (hasManifest())
        {
            String version = manifest.getMainAttributes().getValue("Project-Version");

            if (version != null)
            {
                return version;
            }

            return "0.0.0 (Project version not found in manifest)";
        }

        return "0.0.0 (Development version)";
    }

    private void loadManifest()
    {
        Class<?> type = this.getClass();
        String name = type.getSimpleName() + ".class";
        URL classUrl = type.getResource(name);

        if (classUrl != null && classUrl.getProtocol().startsWith("jar"))
        {
            String path = classUrl.toString();
            String manifestPath = path.substring(0, path.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
            try
            {
                manifest = new Manifest(new URL(manifestPath).openStream());
            } catch (IOException e)
            {
                throw new ConfigurationException(e);
            }
        }
    }

    private boolean hasManifest()
    {
        return (manifest != null);
    }
}
