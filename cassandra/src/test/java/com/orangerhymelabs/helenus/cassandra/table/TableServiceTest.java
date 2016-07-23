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
package com.orangerhymelabs.helenus.cassandra.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.database.Database;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableServiceTest
{
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final String DATABASE = "test_db";

	private static KeyspaceSchema keyspace;
	private static TableService tables;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		TableRepository tableRepository = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseRepository databaseRepository = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		tables = new TableService(databaseRepository, tableRepository);

		Database db = new Database();
		db.name(DATABASE);
		databaseRepository.create(db);
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCRUDSynchronously()
	throws Exception
	{
		// Create
		Table entity = new Table();
		entity.name("table1");
		entity.database(DATABASE);
		entity.description("a test table");
		Table createResult = tables.create(entity);
		assertEquals(entity, createResult);

		// Read
		Table result = tables.read(entity.databaseName(), entity.name());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		entity.description("an updated test table");
		Table updateResult = tables.update(entity);
		assertEquals(entity, updateResult);

		// Re-Read
		Table result2 = tables.read(entity.databaseName(), entity.name());
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		tables.delete(entity.databaseName(), entity.name());

		// Re-Read
		try
		{
			tables.read(entity.databaseName(), entity.name());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Table not deleted: " + entity.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDAsync()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("table2");
		entity.description("another test table");
		TestCallback<Table> callback = new TestCallback<Table>();

		// Create
		tables.createAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		tables.readAsync(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertEquals(entity, callback.entity());

		// Update
		callback.clear();
		entity.description("an updated test table");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		tables.readAsync(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		Table result2 = callback.entity();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		tables.deleteAsync(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		tables.readAsync(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		Table entity = new Table();
		entity.name("table3");
		entity.database(DATABASE);
		Table createResult = tables.create(entity);
		assertEquals(entity, createResult);

		tables.create(entity);
	}

	@Test
	public void shouldThrowOnDuplicateAsync()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("table4");
		TestCallback<Table> callback = new TestCallback<Table>();

		// Create
		tables.createAsync(entity, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		tables.createAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentTableSynchronously()
	{
		tables.read(DATABASE, "doesn't exist");
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentDatabaseSynchronously()
	{
		tables.read("db9", "doesn't matter");
	}

	@Test
	public void shouldThrowOnReadNonExistentAsync()
	throws InterruptedException
	{
		// Table doesn't exist
		TestCallback<Table> callback = new TestCallback<Table>();
		tables.readAsync(DATABASE, "doesn't exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		// Database name doesn't exist
		callback.clear();
		tables.readAsync("db9", "doesn't matter", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentTableSynchronously()
	{
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("doesnt_exist");
		tables.update(entity);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentDatabaseSynchronously()
	{
		Table entity = new Table();
		entity.database("db9");
		entity.name("doesnt_matter");
		tables.update(entity);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("doesnt_exist");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		callback.clear();
		entity.database("db9");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ValidationException.class)
	public void shouldThrowOnUpdateInvalidDatabaseSynchronously()
	{
		Table entity = new Table();
		entity.database("invalid db 9");
		entity.name("doesnt_matter");
		tables.update(entity);
	}

	@Test(expected=ValidationException.class)
	public void shouldThrowOnUpdateInvalidTableSynchronously()
	{
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("isn't valid");
		tables.update(entity);
	}

	@Test
	public void shouldThrowOnInvalidDatabaseNameAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database("invalid db 8");
		entity.name("doesnt_matter");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);
	}

	@Test
	public void shouldThrowOnInvalidTableNameAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("isn't valid");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);
	}

	private void waitFor(TestCallback<Table> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
