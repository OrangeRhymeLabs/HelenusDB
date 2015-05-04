package com.orangerhymelabs.orangedb.document;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentCreatedEvent
extends AbstractEvent<Document>
{
	public DocumentCreatedEvent(Document document)
	{
		super(document);
	}
}
