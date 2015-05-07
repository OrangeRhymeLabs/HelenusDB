package com.orangerhymelabs.orangedb.cassandra.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.strategicgains.eventing.EventHandler;

/**
 *
 * @author udeyoje
 * @since Nov 19, 2014
 */
public class TableDeleteHandler
        implements EventHandler
{

    private Session dbSession;
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDeleteHandler.class);

    public TableDeleteHandler(Session dbSession)
    {
        this.dbSession = dbSession;
    }   
    

    @Override
    public void handle(Object event) throws Exception
    {
        handle((TableDeletedEvent) event);
    }

    public void handle(TableDeletedEvent event)
    {
        LOGGER.info("Cleaning up Indexes for table: " + event.data.databaseName() + "/" + event.data.name());
        //remove all the collections and all the documents in that table.
        //TODO: version instead of delete
        //Delete all indexes
    }

    @Override
    public boolean handles(Class<?> eventClass)
    {
        return TableDeletedEvent.class.isAssignableFrom(eventClass);
    }
}
