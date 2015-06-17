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
package com.orangerhymelabs.orangedb.cassandra.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.TestCallback;
import com.orangerhymelabs.orangedb.cassandra.database.Database;
import com.orangerhymelabs.orangedb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class IndexServiceTest
{
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final String DATABASE_NAME = "test_db";
	private static final String TABLE_NAME = "test_tbl";

	private static KeyspaceSchema keyspace;
	private static IndexService indexes;
	private static Table tbl;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		new IndexRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		TableRepository tableRepository = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseRepository databaseRepository = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		IndexRepository indexRepository = new IndexRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		indexes = new IndexService(indexRepository, tableRepository);

		Database db = new Database();
		db.name(DATABASE_NAME);
		databaseRepository.create(db);

		tbl = new Table();
		tbl.database(db);
		tbl.name(TABLE_NAME);
		tableRepository.create(tbl);
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
		Index index = new Index();
		index.name("index1");
		index.table(tbl);
		index.description("a test index");
		index.fields(Arrays.asList("foo:int"));
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		// Read
		Index result = indexes.read(index.databaseName(), index.tableName(), index.name());
		assertEquals(index, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		index.description("an updated test index");
		Index updateResult = indexes.update(index);
		assertEquals(index, updateResult);

		// Re-Read
		Index result2 = indexes.read(index.databaseName(), index.tableName(), index.name());
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		indexes.delete(index.databaseName(), index.tableName(), index.name());

		// Re-Read
		try
		{
			indexes.read(index.databaseName(), index.tableName(), index.name());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Index not deleted: " + index.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDAsync()
	throws InterruptedException
	{
		Index index = new Index();
		index.table(tbl);
		index.name("index2");
		index.description("another test index");
		index.fields(Arrays.asList("foo:int"));
		TestCallback<Index> callback = new TestCallback<Index>();

		// Create
		indexes.createAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		indexes.readAsync(index.databaseName(), index.tableName(), index.name(), callback);
		waitFor(callback);

		assertEquals(index, callback.entity());

		// Update
		callback.clear();
		index.description("an updated test index");
		indexes.updateAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		indexes.readAsync(index.databaseName(), index.tableName(), index.name(), callback);
		waitFor(callback);

		Index result2 = callback.entity();
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		indexes.deleteAsync(index.databaseName(), index.tableName(), index.name(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		indexes.readAsync(index.databaseName(), index.tableName(), index.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		Index index = new Index();
		index.name("index3");
		index.table(tbl);
		index.fields(Arrays.asList("foo:int"));
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		indexes.create(index);
	}

	@Test
	public void shouldThrowOnDuplicateAsync()
	throws InterruptedException
	{
		Index index = new Index();
		index.table(tbl);
		index.name("index4");
		index.fields(Arrays.asList("foo:int"));
		TestCallback<Index> callback = new TestCallback<Index>();

		// Create
		indexes.createAsync(index, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		indexes.createAsync(index, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentIndexSynchronously()
	{
		indexes.read(DATABASE_NAME, TABLE_NAME, "doesnt_exist");
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentDatabaseSynchronously()
	{
		indexes.read("db9", TABLE_NAME, "index1");
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentTableSynchronously()
	{
		indexes.read(DATABASE_NAME, "doesnt_exist", "index1");
	}

	@Test
	public void shouldThrowOnReadNonExistentAsync()
	throws InterruptedException
	{
		// Index doesn't exist
		TestCallback<Index> callback = new TestCallback<Index>();
		indexes.readAsync(DATABASE_NAME, TABLE_NAME, "doesnt_exist", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		// Database name doesn't exist
		callback.clear();
		indexes.readAsync("db9", TABLE_NAME, "index1", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		// Table name doesn't exist
		callback.clear();
		indexes.readAsync(DATABASE_NAME, "doesnt_exist", "index1", callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentTableSynchronously()
	{
		Index entity = new Index();
		entity.table(DATABASE_NAME, "doesnt_exist", FieldType.UUID);
		entity.name("index1");
		entity.fields(Arrays.asList("foo:int"));
		indexes.update(entity);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentDatabaseSynchronously()
	{
		Index entity = new Index();
		entity.table("db9", TABLE_NAME, FieldType.UUID);
		entity.name("index1");
		entity.fields(Arrays.asList("foo:int"));
		indexes.update(entity);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsync()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		Index entity = new Index();
		entity.table(tbl);
		entity.name("doesnt_exist");
		entity.fields(Arrays.asList("foo:int"));
		indexes.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);

		callback.clear();
		entity.table("db9", TABLE_NAME, FieldType.UUID);
		indexes.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateInvalidDatabaseSynchronously()
	{
		Index entity = new Index();
		entity.table("invalid db 9", TABLE_NAME, FieldType.UUID);
		entity.name("doesnt_matter");
		indexes.update(entity);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateInvalidTableSynchronously()
	{
		Index entity = new Index();
		entity.table(DATABASE_NAME, "Isn't valid", FieldType.UUID);
		entity.name("doesnt_matter");
		indexes.update(entity);
	}

	@Test
	public void shouldThrowOnInvalidDatabaseNameAsync()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		Index entity = new Index();
		entity.table("invalid db 8", TABLE_NAME, FieldType.UUID);
		entity.name("doesnt_matter");
		indexes.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnInvalidIndexNameAsync()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		Index entity = new Index();
		entity.table(tbl);
		entity.name("isn't valid");
		indexes.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ValidationException);
	}

	private void waitFor(TestCallback<Index> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
