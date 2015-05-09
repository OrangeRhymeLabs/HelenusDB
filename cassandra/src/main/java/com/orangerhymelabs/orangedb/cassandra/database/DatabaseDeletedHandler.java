package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;
import com.strategicgains.eventing.EventHandler;

/**
 *
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseDeletedHandler
implements EventHandler
{

	private Session dbSession;
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseDeletedHandler.class);

	public DatabaseDeletedHandler(Session dbSession)
	{
		this.dbSession = dbSession;
	}

	@Override
	public void handle(Object event) throws Exception
	{
		handle((DatabaseDeletedEvent) event);
	}

	public void handle(DatabaseDeletedEvent event)
	{
		// remove all the collections and all the documents in that database.
		// TODO: version instead of delete
		// tables
		LOGGER.info("Cleaning up tables for database: " + event.data.name());
//		TableRepository tr = new TableRepository(dbSession);
//		List<Table> tables = tr.readAll(event.data.name());// get all tables
//		for (Table t : tables)
//		{
//			// tr.doDelete(t);// then delete them
//		}
	}

	@Override
	public boolean handles(Class<?> eventClass)
	{
		return DatabaseDeletedEvent.class.isAssignableFrom(eventClass);
	}
}
