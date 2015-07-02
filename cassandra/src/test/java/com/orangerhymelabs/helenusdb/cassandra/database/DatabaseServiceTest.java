/*
    Copyright 2015, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.orangerhymelabs.helenusdb.cassandra.database;

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

import com.orangerhymelabs.helenusdb.cassandra.CassandraManager;
import com.orangerhymelabs.helenusdb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenusdb.cassandra.TestCallback;
import com.orangerhymelabs.helenusdb.cassandra.database.Database;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenusdb.exception.DuplicateItemException;
import com.orangerhymelabs.helenusdb.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
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
	public void shouldCRUDDatabaseSync()
	throws Exception
	{
		// Create
		Database entity = new Database();
		entity.name("db1");
		entity.description("a test database");
		Database createResult = databases.create(entity);
		assertEquals(entity, createResult);

		// Read
		Database result = databases.read(entity.name());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		entity.description("an updated test database");
		Database updateResult = databases.update(entity);
		assertEquals(entity, updateResult);

		// Re-Read
		Database result2 = databases.read(entity.name());
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		databases.delete(entity.name());

		// Re-Read
		try
		{
			databases.read(entity.name());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Database not deleted: " + entity.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDDatabaseAsync()
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
		databases.readAsync(entity.name(), callback);
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
		databases.readAsync(entity.name(), callback);
		waitFor(callback);

		Database result2 = callback.entity();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		databases.deleteAsync(entity.name(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		databases.readAsync(entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateDatabaseSync()
	{
		// Create
		Database entity = new Database();
		entity.name("db3");
		Database createResult = databases.create(entity);
		assertEquals(entity, createResult);

		databases.create(entity);
	}

	@Test
	public void shouldThrowOnDuplicateDatabaseAsync()
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

	@Test
	public void shouldThrowOnReadNonExistentDatabaseAsync()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		databases.readAsync("doesn't exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentDatabaseSync()
	{
		databases.read("doesn't exist");
	}

	@Test
	public void shouldThrowOnUpdateNonExistentDatabaseAsync()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesnt_exist");
		databases.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentDatabaseSync()
	{
		Database entity = new Database();
		entity.name("doesnt_exist");
		databases.update(entity);
	}

	@Test
	public void shouldThrowOnInvalidDatabaseNameAsync()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);

		// Should be same for create.
		callback.clear();
		databases.createAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);
	}

	@Test(expected=ValidationException.class)
	public void shouldThrowOnInvalidDatabaseNameUpdateSync()
	{
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.update(entity);
	}

	@Test(expected=ValidationException.class)
	public void shouldThrowOnInvalidDatabaseNameCreateSync()
	{
		Database entity = new Database();
		entity.name("doesn't exist");
		databases.create(entity);
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
