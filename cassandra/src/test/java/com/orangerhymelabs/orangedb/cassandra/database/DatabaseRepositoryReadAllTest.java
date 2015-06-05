package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.TestCallback;

public class DatabaseRepositoryReadAllTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static DatabaseRepository databases;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		databases = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
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
		populateDatabase("dba", 10);
		shouldReturnListSynchronously();
		shouldReturnListAsynchronously();
	}

	private void shouldReturnEmptyListSynchronously()
    {
		List<Database> dbs = databases.readAll();
		assertNotNull(dbs);
		assertTrue(dbs.isEmpty());
    }

	private void shouldReturnEmptyListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<Database>> callback = new TestCallback<List<Database>>();
		databases.readAllAsync(callback);
		waitFor(callback);

		assertFalse(callback.isEmpty());
		assertTrue(callback.entity().isEmpty());
    }

	private void populateDatabase(String prefix, int count)
    {
		Database db = new Database();

		for (int i = 1; i <= count; i++)
		{
			db.name(prefix + i);
			databases.create(db);
		}
    }

	private void shouldReturnListSynchronously()
    {
		List<Database> dbs = databases.readAll();
		assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(10, dbs.size());
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<Database>> callback = new TestCallback<List<Database>>();
		databases.readAllAsync(callback);
		waitFor(callback);

		List<Database> dbs = callback.entity();
		assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(10, dbs.size());
    }

	private void waitFor(TestCallback<List<Database>> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
