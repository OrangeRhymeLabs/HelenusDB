package com.orangerhymelabs.orangedb.cassandra.database;

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
		populateDatabase();
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
		DatabaseCallback callback = new DatabaseCallback();
		databases.readAllAsync(callback);
		waitFor(callback);

		assertFalse(callback.isEmpty());
		assertTrue(callback.databases().isEmpty());
    }

	private void populateDatabase()
    {
		Database db = new Database();
		db.name("dba1");
		databases.create(db);
		db.name("dba2");
		databases.create(db);
		db.name("dba3");
		databases.create(db);
		db.name("dba4");
		databases.create(db);
    }

	private void shouldReturnListSynchronously()
    {
		List<Database> dbs = databases.readAll();
		assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(4, dbs.size());
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		DatabaseCallback callback = new DatabaseCallback();
		databases.readAllAsync(callback);
		waitFor(callback);

		List<Database> dbs = callback.databases();
		assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(4, dbs.size());
    }

	private void waitFor(DatabaseCallback callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }

	private class DatabaseCallback
	implements ResultCallback<List<Database>>
	{
		private List<Database> databases;
		private Throwable throwable;

		@Override
        public void onSuccess(List<Database> result)
        {
			this.databases = result;
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
			this.databases = null;
			this.throwable = null;
		}

		public boolean isEmpty()
		{
			return (databases == null && throwable == null);
		}

		public List<Database> databases()
		{
			return databases;
		}

		public Throwable throwable()
		{
			return throwable;
		}
	}
}
