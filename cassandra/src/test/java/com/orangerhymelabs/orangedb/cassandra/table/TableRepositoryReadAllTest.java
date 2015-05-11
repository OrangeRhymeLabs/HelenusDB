package com.orangerhymelabs.orangedb.cassandra.table;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

public class TableRepositoryReadAllTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static TableRepository tables;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		tables = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldReadAll()
	throws InterruptedException
	{
		shouldReturnEmptyListSynchronously();
		shouldReturnEmptyListAsynchronously();
		populateDatabase();
		shouldReturnListSynchronously();
		shouldReturnListAsynchronously();
	}

	private void shouldReturnEmptyListSynchronously()
    {
		List<Table> tbs = tables.readAll("database1");
		assertNotNull(tbs);
		assertTrue(tbs.isEmpty());
    }

	private void shouldReturnEmptyListAsynchronously()
	throws InterruptedException
    {
		ReallAllCallback callback = new ReallAllCallback();
		tables.readAllAsync(callback, "database1");
		waitFor(callback);

		assertFalse(callback.isEmpty());
		assertTrue(callback.entities().isEmpty());
    }

	private void populateDatabase()
    {
		Table table = new Table();
		table.database("database1");
		table.name("table1");
		tables.create(table);
		table.name("table2");
		tables.create(table);
		table.name("table3");
		tables.create(table);
		table.name("table4");
		tables.create(table);

		table.database("database2");
		table.name("table1");
		tables.create(table);
    }

	private void shouldReturnListSynchronously()
    {
		List<Table> entities = tables.readAll("database1");
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		ReallAllCallback callback = new ReallAllCallback();
		tables.readAllAsync(callback, "database1");
		waitFor(callback);

		List<Table> entities = callback.entities();
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void waitFor(ReallAllCallback callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }

	private class ReallAllCallback
	implements ResultCallback<List<Table>>
	{
		private List<Table> entities;
		private Throwable throwable;

		@Override
        public void onSuccess(List<Table> result)
        {
			this.entities = result;
			alert();
        }

		@Override
        public void onFailure(Throwable t)
        {
			this.throwable = t;
			alert();
        }

		private synchronized void alert()
		{
			notifyAll();
		}

		public void clear()
		{
			this.entities = null;
			this.throwable = null;
		}

		public boolean isEmpty()
		{
			return (entities == null && throwable == null);
		}

		public List<Table> entities()
		{
			return entities;
		}

		public Throwable throwable()
		{
			return throwable;
		}
	}
}
