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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
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
		Database db = new Database();
		db.name("db1");
		db.description("a test database");
		Database createResult = databases.create(db).get();
		assertEquals(db, createResult);

		// Read
		Database result = databases.read(db.identifier()).get();
		assertEquals(db, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		db.description("an updated test database");
		Database updateResult = databases.update(db).get();
		assertEquals(db, updateResult);

		// Re-Read
		Database result2 = databases.read(db.identifier()).get();
		assertEquals(db, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		databases.delete(db.identifier()).get();

		// Re-Read
		try
		{
			databases.read(db.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (ItemNotFoundException.class.equals(e.getCause().getClass())) return;
		}

		fail("Database not deleted: " + db.identifier().toString());
	}

	@Test
	public void shouldCreateDatabaseAsynchronously()
	throws InterruptedException
	{
		Database db = new Database();
		db.name("db2");
		db.description("another test database");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Shouldn't Exist
		TestCallback<Boolean> existCallback = new TestCallback<Boolean>();
		Futures.addCallback(databases.exists(db.identifier()), existCallback, MoreExecutors.directExecutor());
		waitFor(existCallback);
		assertNull(existCallback.throwable());
		assertFalse(existCallback.entity());

		// Create
		Futures.addCallback(databases.create(db), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		Futures.addCallback(databases.read(db.identifier()), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertEquals(db, callback.entity());

		// Should Also Exist
		existCallback.clear();
		Futures.addCallback(databases.exists(db.identifier()), existCallback, MoreExecutors.directExecutor());
		waitFor(existCallback);
		assertNull(existCallback.throwable());
		assertTrue(existCallback.entity());

		// Update
		callback.clear();
		db.description("an updated test database");
		Futures.addCallback(databases.update(db), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		Futures.addCallback(databases.read(db.identifier()), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		Database result2 = callback.entity();
		assertEquals(db, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<Boolean>();
		Futures.addCallback(databases.delete(db.identifier()), deleteCallback, MoreExecutors.directExecutor());
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		Futures.addCallback(databases.read(db.identifier()), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateDatabaseSynchronously()
	throws Throwable
	{
		// Create
		Database entity = new Database();
		entity.name("db3");
		Database createResult = databases.create(entity).get();
		assertEquals(entity, createResult);

		try
		{
			databases.create(entity).get();
		}
		catch(ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnDuplicateDatabaseAynchronously()
	throws InterruptedException
	{
		Database entity = new Database();
		entity.name("db4");
		TestCallback<Database> callback = new TestCallback<Database>();

		// Create
		Futures.addCallback(databases.create(entity), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNotNull(callback.entity());
		assertNull(callback.throwable());

		// Create Duplicate
		Futures.addCallback(databases.create(entity), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentDatabaseSynchronously()
	throws Throwable
	{
		try
		{
			databases.read(new Identifier("doesn't exist")).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnReadNonExistentDatabaseAsynchronously()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Futures.addCallback(databases.read(new Identifier("doesn't exist")), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentDatabaseSynchronously()
	throws Throwable
	{
		Database entity = new Database();
		entity.name("doesn't exist");

		try
		{
			databases.update(entity).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnUpdateNonExistentDatabaseAsynchronously()
	throws InterruptedException
	{
		TestCallback<Database> callback = new TestCallback<Database>();
		Database entity = new Database();
		entity.name("doesn't exist");
		Futures.addCallback(databases.update(entity), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
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
