package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.orangerhymelabs.orangedb.persistence.Identifier;

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
	public void shouldCRUDDatabaseSynchronously()
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
		try
		{
			databases.read(entity.getId());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Database not deleted: " + entity.getId().toString());
	}

	@Test
	public void shouldCreateDatabaseAsynchronously()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db2");
		entity.description("another test database");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		databases.createAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitFor(callback);

		assertEquals(entity, callback.entity());

		// Update
		callback.clear();
		entity.description("an updated test database");
		databases.updateAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitFor(callback);

		Database result2 = callback.entity();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		databases.deleteAsync(entity.getId(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		databases.readAsync(entity.getId(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateDatabaseSynchronously()
	{
		// Create
		Database entity = new Database();
		entity.name("db3");
		Database createResult = databases.create(entity);
		assertEquals(entity, createResult);

		databases.create(entity);
	}

	@Test
	public void shouldThrowOnDuplicateDatabaseAynchronously()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db4");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		databases.createAsync(entity, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		databases.createAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentDatabaseSynchronously()
	{
		databases.read(new Identifier("doesn't exist"));
	}

	@Test
	public void shouldThrowOnReadNonExistentDatabaseAsynchronously()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		databases.readAsync(new Identifier("doesn't exist"), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentDatabaseSynchronously()
	{
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.update(entity);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentDatabaseAsynchronously()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	private void waitFor(TestCallback<Database> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
