package com.orangerhymelabs.orangedb.table;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableDeletedEvent
extends AbstractEvent<Table>
{
	public TableDeletedEvent(Table table)
	{
		super(table);
	}
}
