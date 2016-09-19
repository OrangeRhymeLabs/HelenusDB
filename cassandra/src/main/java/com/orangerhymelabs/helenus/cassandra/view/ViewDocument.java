package com.orangerhymelabs.helenus.cassandra.view;

import org.bson.BSONObject;

import com.orangerhymelabs.helenus.cassandra.document.Document;
import com.orangerhymelabs.helenus.persistence.AbstractEntity;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * A document contained in a materialized view (keyed on different properties).
 * 
 * @author tfredrich
 */
public class ViewDocument
extends AbstractEntity
{
	@Required("Identifier")
	private Identifier identifier;

	@Required("Document ID")
	private Identifier documentId;

	// The BSON document.
	private BSONObject bson;

	public ViewDocument()
	{
		super();
	}

	public ViewDocument(Document document)
	{
		this();
		document(document);
	}

	public void document(Document document)
	{
		documentId(document.identifier());
		object(document.object());
	}

	public Document document()
	{
		Document d = new Document(object());
		d.identifier(documentId());
		return d;
	}

	@Override
	public Identifier identifier()
	{
		return (identifier != null ? new Identifier(identifier) : null);
	}

	public void identifier(Identifier id)
	{
		this.identifier = (id != null ? new Identifier(id) : null);
	}

	public Identifier documentId()
	{
		return documentId;
	}

	public void documentId(Identifier id)
	{
		this.documentId = id;
	}

	public boolean hasObject()
	{
		return (bson != null);
	}

	public BSONObject object()
	{
		return bson;
	}

	public void object(BSONObject bson)
	{
		this.bson = bson;
	}

	@Override
	public String toString()
	{
		return "View{Identifier=" + identifier.toString() + ", " + document().toString() + "}";
	}
}
