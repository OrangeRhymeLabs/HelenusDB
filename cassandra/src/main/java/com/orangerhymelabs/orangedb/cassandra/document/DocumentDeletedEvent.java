package com.orangerhymelabs.orangedb.cassandra.document;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DocumentDeletedEvent
extends AbstractEvent<Identifier>
{
	public DocumentDeletedEvent(Identifier id)
	{
		super(id);
	}
}
