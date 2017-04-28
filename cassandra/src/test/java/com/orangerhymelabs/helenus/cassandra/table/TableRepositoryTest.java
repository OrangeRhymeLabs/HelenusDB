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

import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.Futures;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableRepositoryTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static TableRepository tables;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		tables = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
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
		Table table = new Table();
		table.name("table1");
		table.database("db1");
		table.description("a test table");
		Table createResult = tables.create(table).get();
		assertEquals(table, createResult);

		// Document table should exist.
		assertTrue("Document table not created: " + table.toDbTable(), tableExists(table.toDbTable()));

		// Read
		Table result = tables.read(table.identifier()).get();
		assertEquals(table, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		table.description("an updated test table");
		Table updateResult = tables.update(table).get();
		assertEquals(table, updateResult);

		// Re-Read
		Table result2 = tables.read(table.identifier()).get();
		assertEquals(table, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		tables.delete(table.identifier());

		// Document table should no longer exist.
		assertFalse("Document table not deleted: " + table.toDbTable(), tableExists(table.toDbTable()));

		// Re-Read table
		try
		{
			tables.read(table.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (ItemNotFoundException.class.equals(e.getCause().getClass())) return;
		}

		fail("Table not deleted: " + table.identifier().toString());
	}

	@Test
	public void shouldCRUDAsynchronously()
	throws InterruptedException
	{
		Table table = new Table();
		table.database("db2");
		table.name("table2");
		table.description("another test table");
		TestCallback<Table> callback = new TestCallback<Table>();

		// Create
		Futures.addCallback(tables.create(table), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Document table should exist.
		assertTrue("Document table not created: " + table.toDbTable(), tableExists(table.toDbTable()));

		// Read
		callback.clear();
		Futures.addCallback(tables.read(table.identifier()), callback);
		waitFor(callback);

		assertEquals(table, callback.entity());

		// Update
		callback.clear();
		table.description("an updated test table");
		Futures.addCallback(tables.update(table), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		Futures.addCallback(tables.read(table.identifier()), callback);
		waitFor(callback);

		Table result2 = callback.entity();
		assertEquals(table, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		waitFor(callback);
		TestCallback<Boolean> deleteCallback = new TestCallback<Boolean>();
		Futures.addCallback(tables.delete(table.identifier()), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Document table should no longer exist.
		assertFalse("Document table not deleted: " + table.toDbTable(), tableExists(table.toDbTable()));

		// Re-Read
		callback.clear();
		Futures.addCallback(tables.read(table.identifier()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	throws Throwable
	{
		// Create
		Table table = new Table();
		table.name("table3");
		table.database("db3");
		Table createResult = tables.create(table).get();
		assertEquals(table, createResult);

		try
		{
			tables.create(table).get();
		}
		catch(ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		Table table = new Table();
		table.name("table4");
		table.database("db4");
		TestCallback<Table> callback = new TestCallback<Table>();

		// Create
		Futures.addCallback(tables.create(table), callback);
		waitFor(callback);

		assertNotNull(callback.entity());
		assertNull(callback.throwable());

		// Create Duplicate
		Futures.addCallback(tables.create(table), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=StorageException.class)
	public void shouldThrowOnBadKeys()
	throws InterruptedException
	{
		Table table = new Table();
		table.name("table4");
		table.database("db4");
		table.keys("id:invalid_type");

		tables.create(table);
	}

	@Test
	public void shouldMarshal()
	throws InterruptedException, ExecutionException
	{
		// Create
		Table table = new Table();
		table.name("table5");
		table.database("db5");
		table.keys("id:uuid");
		Table createResult = tables.create(table).get();
		assertEquals(table, createResult);

		Table sync = tables.read(table.identifier()).get();
		assertEquals(table.createdAt(), sync.createdAt());
		assertEquals(table.updatedAt(), sync.updatedAt());
		assertEquals(table.databaseName(), sync.databaseName());
		assertEquals(table.description(), sync.description());
		assertEquals(table.keys(), sync.keys());
		assertEquals(table.name(), sync.name());
		assertEquals(table.ttl(), sync.ttl());

		TestCallback<Table> callback = new TestCallback<Table>();
		Futures.addCallback(tables.read(table.identifier()), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		Table async = callback.entity();
		assertEquals(table.createdAt(), async.createdAt());
		assertEquals(table.updatedAt(), async.updatedAt());
		assertEquals(table.databaseName(), async.databaseName());
		assertEquals(table.description(), async.description());
		assertEquals(table.keys(), async.keys());
		assertEquals(table.name(), async.name());
		assertEquals(table.ttl(), async.ttl());
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	throws Throwable
	{
		try
		{
			tables.read(new Identifier("db5", "doesn't exist")).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Futures.addCallback(tables.read(new Identifier("db6", "doesn't exist")), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	throws Throwable
	{
		Table table = new Table();
		table.database("db7");
		table.name("doesn't exist");

		try
		{
			tables.update(table).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table table = new Table();
		table.database("db8");
		table.name("doesn't exist");
		Futures.addCallback(tables.update(table), callback);
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

	private boolean tableExists(String tableName)
    {
	    ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system_schema.tables where keyspace_name='%s' and table_name='%s'", CassandraManager.keyspace(), tableName));
		return (rs.one().getLong(0) > 0);
    }
}
