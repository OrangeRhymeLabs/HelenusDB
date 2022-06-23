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
package com.orangerhymelabs.helenus.cassandra.database;

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

import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
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
	public void shouldCRUDDatabaseAsync()
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
		TestCallback<Boolean> deleteCallback = new TestCallback<Boolean>();
		databases.delete(entity.name(), deleteCallback);
		waitFor(deleteCallback);

		assertNull(deleteCallback.throwable());
		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		databases.read(entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnDuplicateDatabaseAsync()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db4");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		databases.create(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		// Create Duplicate
		databases.create(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldThrowOnReadNonExistentDatabaseAsync()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		databases.read("doesn't exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentDatabaseAsync()
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
	public void shouldThrowOnInvalidDatabaseNameAsync()
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
