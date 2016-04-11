package com.orangerhymelabs.helenusdb.rest.document;

import org.bson.BSON;
import org.restexpress.ContentType;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.exception.BadRequestException;

import com.orangerhymelabs.helenus.cassandra.document.Document;
import com.orangerhymelabs.helenus.cassandra.document.DocumentService;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenusdb.rest.Constants;
import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.hyperexpress.builder.TokenResolver;
import com.strategicgains.hyperexpress.builder.UrlBuilder;

import io.netty.handler.codec.http.HttpMethod;

public class DocumentController
{

	private static final UrlBuilder LOCATION_BUILDER = new UrlBuilder();

	private DocumentService documents;

	public DocumentController(DocumentService documentsService)
	{
		super();
		this.documents = documentsService;
	}

	public Document create(Request request, Response response)
	{
		String database = request.getHeader(Constants.Url.DATABASE, "No database provided");
		String table = request.getHeader(Constants.Url.TABLE, "No table provided");
		String data = request.getBody().toString(ContentType.CHARSET);

		if (data == null || data.isEmpty())
		{
			throw new BadRequestException("No document data provided");
		}

		Document saved = documents.create(database, table, new Document(BSON.decode(data.getBytes())));

		// Construct the response for create...
		response.setResponseCreated();

		// enrich the resource with links, etc. here...
		TokenResolver resolver = HyperExpress.bind(Constants.Url.DOCUMENT_ID,
		    saved.id().toString());

		// Include the Location header...
		String locationPattern = request.getNamedUrl(HttpMethod.GET,
		    Constants.Routes.DOCUMENT);
		response.addLocationHeader(
		    LOCATION_BUILDER.build(locationPattern, resolver));

		// Return the newly-created resource...
		return saved;
	}

	public Document read(Request request, Response response)
	{
		String database = request.getHeader(Constants.Url.DATABASE,
		    "No database provided");
		String table = request.getHeader(Constants.Url.TABLE,
		    "No table provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID,
		    "No document ID supplied");
		Document document = documents.read(database, table,
		    new Identifier(database, table, id));

		// enrich the entity with links, etc. here...
		HyperExpress.bind(Constants.Url.DOCUMENT_ID, document.id().toString());

		return document;
	}

	// public List<Document> readAll(Request request, Response response)
	// {
	// String namespace = request.getHeader(Constants.Url.DATABASE, "No
	// namespace provided");
	// String collection = request.getHeader(Constants.Url.TABLE, "No collection
	// provided");
	//
	// HyperExpress.tokenBinder(new TokenBinder<Document>()
	// {
	// @Override
	// public void bind(Document object, TokenResolver resolver)
	// {
	// resolver.bind(Constants.Url.DOCUMENT_ID, object.getUuid().toString());
	// }
	// });
	//
	// return service.readAll(namespace, collection);
	// }

	public void update(Request request, Response response)
	{
		String database = request.getHeader(Constants.Url.DATABASE,
		    "No database provided");
		String table = request.getHeader(Constants.Url.TABLE,
		    "No table provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID,
		    "No document ID supplied");
		String data = request.getBody().toString(ContentType.CHARSET);

		if (data == null || data.isEmpty())
		{
			throw new BadRequestException("No document data provided");
		}

		Document document = new Document();
		document.id(id);
		document.object(BSON.decode(data.getBytes()));
		documents.update(database, table, document);
		response.setResponseNoContent();
	}

	public void delete(Request request, Response response)
	{
		String database = request.getHeader(Constants.Url.DATABASE,
		    "No database provided");
		String table = request.getHeader(Constants.Url.TABLE,
		    "No table provided");
		String id = request.getHeader(Constants.Url.DOCUMENT_ID,
		    "No document ID supplied");
		documents.delete(database, table, new Identifier(database, table, id));
		response.setResponseNoContent();
	}
}
