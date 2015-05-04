package com.orangerhymelabs.orangedb.document;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentUpdatedEvent
extends AbstractEvent<Document>
{
	public DocumentUpdatedEvent(Document document)
	{
		super(document);
	}
}
