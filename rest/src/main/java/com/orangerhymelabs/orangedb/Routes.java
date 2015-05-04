package com.orangerhymelabs.orangedb;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import org.restexpress.RestExpress;

public abstract class Routes
{
    public static void define(Configuration config, RestExpress server)
    {
        server.uri("/", config.getDatabaseController())
                .action("readAll", GET)
                .method(OPTIONS)
                .name(Constants.Routes.DATABASES);

        server.uri("/{database}", config.getDatabaseController())
                .method(GET, DELETE, PUT, POST)
                .method(OPTIONS)
                .name(Constants.Routes.DATABASE);

        server.uri("/{database}/", config.getTableController())
                .action("readAll", GET)
                .name(Constants.Routes.TABLES);

        server.uri("/{database}/{table}", config.getTableController())
                .method(GET, DELETE, PUT, POST)
                .name(Constants.Routes.TABLE);

        server.uri("/{database}/{table}/", config.getDocumentController())
                .method(POST)
                .name(Constants.Routes.DOCUMENTS);

        server.uri("/{database}/{table}/{documentId}", config.getDocumentController())
                .method(GET, PUT, DELETE)
                .name(Constants.Routes.DOCUMENT);
    }
}
