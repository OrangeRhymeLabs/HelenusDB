package com.orangerhymelabs.helenusdb.rest.table;

import java.util.List;

import org.restexpress.Request;
import org.restexpress.Response;

import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenusdb.rest.Constants;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenBinder;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;

import io.netty.handler.codec.http.HttpMethod;

public class TableController
{
	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private TableService service;
	
	public TableController(TableService collectionsService)
	{
		super();
		this.service = collectionsService;
	}

	public Table create(Request request, Response response)
	{
		String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
		String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
		Table table = request.getBodyAs(Table.class);

		if (table == null)
		{
			table = new Table();
		}

		table.database(databaseName);
		table.name(tableName);
		Table saved = service.create(table);

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.TABLE, saved.name());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET, Constants.Routes.TABLE);
		response.addLocationHeader(LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Table read(Request request, Response response)
	{
		String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
		String tableName = request.getHeader(Constants.Url.TABLE, "No table supplied");
		Table table = service.read(databaseName, tableName);
		HyperExpress.bind(Constants.Url.TABLE, table.name());
		return table;
	}

	public List<Table> readAll(Request request, Response response)
	{
		String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");

		HyperExpress.tokenBinder(new TokenBinder<Table>()
		{
			@Override
            public void bind(Table object, TokenResolver resolver)
            {
				resolver.bind(Constants.Url.TABLE, object.name());
			}
		});

		return service.readAll(databaseName);
	}

	public void update(Request request, Response response)
	{
		String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
		String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
		Table table = request.getBodyAs(Table.class, "Table details not provided");
		table.database(databaseName);
		table.name(tableName);
		service.update(table);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String databaseName = request.getHeader(Constants.Url.DATABASE, "No database provided");
		String tableName = request.getHeader(Constants.Url.TABLE, "No table provided");
		service.delete(databaseName, tableName);
		response.setResponseNoContent();
	}
}
