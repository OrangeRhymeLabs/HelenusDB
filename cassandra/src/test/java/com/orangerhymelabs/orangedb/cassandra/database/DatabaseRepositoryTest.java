package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

public class DatabaseRepositoryTest
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
	public void shouldCRUDDatabaseSyncronously()
	throws Exception
	{
		// Create
		Database entity = new Database();
		entity.name("db1");
		entity.description("a test database");
		Database createResult = databases.create(entity);
		assertEquals(entity, createResult);

		// Read
		Database result = databases.read(entity.getId());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		entity.description("an updated test database");
		Database updateResult = databases.update(entity);
		assertEquals(entity, updateResult);

		// Re-Read
		Database result2 = databases.read(entity.getId());
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		databases.delete(entity.getId());

		// Re-Read
		Database result3 = databases.read(entity.getId());
		assertNull(result3);
	}

	@Test
	public void shouldCreateDatabaseAsyncronously()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db2");
		entity.description("another test database");
		DatabaseCallback callback = new DatabaseCallback();

		// Create
		databases.createAsync(entity, callback);
		waitOn(callback);

		assertNull(callback.throwable());
//		assertEquals(entity, callback.database());

		// Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitOn(callback);

		assertEquals(entity, callback.database());

		// Update
		callback.clear();
		entity.description("an updated test database");
		databases.updateAsync(entity, callback);
		waitOn(callback);

		assertNull(callback.throwable());
//		assertEquals(entity, callback.database());

		// Re-Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitOn(callback);

		Database result2 = callback.database();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		databases.deleteAsync(entity.getId(), callback);
		waitOn(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitOn(callback);
		
		assertTrue(callback.isEmpty());
	}

	private void waitOn(DatabaseCallback callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }

	private class DatabaseCallback
	implements ResultCallback<Database>
	{
		private Database database;
		private Throwable throwable;

		@Override
        public void onSuccess(Database result)
        {
			this.database = result;
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
			this.database = null;
			this.throwable = null;
		}

		public boolean isEmpty()
		{
			return (database == null && throwable == null);
		}

		public Database database()
		{
			return database;
		}

		public Throwable throwable()
		{
			return throwable;
		}
	}
}
