package com.orangerhymelabs.helenusdb.rest.database;

import static com.orangerhymelabs.helenusdb.rest.Constants.Routes.DATABASE;
import static com.orangerhymelabs.helenusdb.rest.Constants.Routes.DATABASES;

import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.orangerhymelabs.helenusdb.cassandra.database.Database;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenusdb.rest.Constants;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

/**
 * REST controller for Database entities.
 */
public class DatabaseController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private DatabaseService databases;
	
	public DatabaseController(DatabaseService databaseService)
	{
		super();
		this.databases = databaseService;
	}

	public void options(Request request, Response response)
	{
		if (DATABASES.equals(request.getResolvedRoute().getName()))
		{
			response.addHeader(HttpHeaders.Names.ALLOW, "GET");
		}
		else if (DATABASE.equals(request.getResolvedRoute().getName()))
		{
			response.addHeader(HttpHeaders.Names.ALLOW, "GET, DELETE, PUT, POST");			
		}
	}

	public Database create(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
		Database database = request.getBodyAs(Database.class);

		if (database == null)
		{
			database = new Database();
		}

		database.name(name);
		Database saved = databases.create(database);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.DATABASE, saved.name());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.DATABASE);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Database read(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.DATABASE, "No database provided");
		Database database = databases.read(name);

		// enrich the entity with links, etc. here...
		HyperExpress.bind(Constants.Url.DATABASE, database.name());

		return database;
	}

	public List<Database> readAll(Request request, Response response)
	{
		HyperExpress.tokenBinder(new TokenBinder<Database>()
		{
			@Override
            public void bind(Database object, TokenResolver resolver)
            {
				resolver.bind(Constants.Url.DATABASE, object.name());
            }
		});
		return databases.readAll();
	}

	public void update(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
		Database database = request.getBodyAs(Database.class, "Database details not provided");

		database.name(name);
		databases.update(database);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String name = request.getHeader(Constants.Url.DATABASE, "No database name provided");
		databases.delete(name);
		response.setResponseNoContent();
	}
}
