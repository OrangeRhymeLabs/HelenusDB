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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.database.Database;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseService;
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
	throws ConfigurationException, TTransportException, IOException, InterruptedException, ExecutionException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		TableRepository tableRepository = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseRepository databaseRepository = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseService databaseService = new DatabaseService(databaseRepository);
		tables = new TableService(databaseService, tableRepository);

		Database db = new Database();
		db.name(DATABASE);
		databaseRepository.create(db).get();
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
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
		tables.create(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		tables.read(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertEquals(entity, callback.entity());

		// Update
		callback.clear();
		entity.description("an updated test table");
		tables.update(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		tables.read(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		Table result2 = callback.entity();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<>();
		tables.delete(entity.databaseName(), entity.name(), deleteCallback);
		waitFor(deleteCallback);

		assertNull(deleteCallback.throwable());
		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		tables.read(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
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
		tables.create(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		// Create Duplicate
		tables.create(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldThrowOnReadNonExistentAsync()
	throws InterruptedException
	{
		// Table doesn't exist
		TestCallback<Table> callback = new TestCallback<Table>();
		tables.read(DATABASE, "doesn't exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		// Database name doesn't exist
		callback.clear();
		tables.read("db9", "doesn't matter", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("doesnt_exist");
		tables.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		callback.clear();
		entity.database("db9");
		tables.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnInvalidDatabaseNameAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database("invalid db 8");
		entity.name("doesnt_matter");
		tables.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
//		assertTrue(callback.throwable() instanceof ValidationException);
	}

	@Test
	public void shouldThrowOnInvalidTableNameAsync()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database(DATABASE);
		entity.name("isn't valid");
		tables.update(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);

		// Should be same for create.
		callback.clear();
		tables.create(entity, callback);
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
