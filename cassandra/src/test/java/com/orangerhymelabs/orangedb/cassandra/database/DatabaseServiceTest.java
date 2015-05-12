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
import com.orangerhymelabs.orangedb.cassandra.TestCallback;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationException;

public class DatabaseServiceTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static DatabaseService databases;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		DatabaseRepository databaseRepository = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		databases = new DatabaseService(databaseRepository);
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCreateDatabase()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db2");
		entity.description("another test database");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		databases.create(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		databases.read(entity.name(), callback);
		waitFor(callback);

		assertEquals(entity, callback.entity());

		// Update
		callback.clear();
		entity.description("an updated test database");
		databases.update(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		databases.read(entity.name(), callback);
		waitFor(callback);

		Database result2 = callback.entity();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		databases.delete(entity.name(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		databases.read(entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnDuplicateDatabase()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db4");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		databases.create(entity, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		databases.create(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldThrowOnReadNonExistentDatabase()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		databases.read("doesn't exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentDatabase()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesnt_exist");
		databases.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnInvalidDatabaseName()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);

		// Should be same for create.
		callback.clear();
		databases.create(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);
	}

	private void waitFor(TestCallback<?> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
