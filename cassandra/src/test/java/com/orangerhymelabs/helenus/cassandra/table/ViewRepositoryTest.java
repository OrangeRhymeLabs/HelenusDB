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
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.view.View;
import com.orangerhymelabs.helenus.cassandra.view.ViewRepository;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ViewRepositoryTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static ViewRepository views;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new ViewRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		views = new ViewRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
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

		View view = new View();
		view.table(table);
		view.name("view1");
		view.description("a test view");
		view.keys("(id:uuid), -foo:timestamp");
		View createResult = views.create(view).get();
		assertEquals(view, createResult);

		// Document table should exist.
		assertTrue("View table not created: " + view.toDbTable(), tableExists(view.toDbTable()));

		// Read
		View result = views.read(view.identifier()).get();
		assertEquals(view, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		view.description("an updated test view");
		View updateResult = views.update(view).get();
		assertEquals(view, updateResult);

		// Re-Read
		View result2 = views.read(view.identifier()).get();
		assertEquals(view, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		views.delete(view.identifier()).get();

		// View table should no longer exist.
		assertFalse("View table not deleted: " + view.toDbTable(), tableExists(view.toDbTable()));

		// Re-Read table
		try
		{
			views.read(view.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (ItemNotFoundException.class.equals(e.getCause().getClass())) return;
		}

		fail("View not deleted: " + view.identifier().toString());
	}

	@Test
	public void shouldCRUDAsynchronously()
	throws InterruptedException
	{
		Table table = new Table();
		table.database("db2");
		table.name("table2");
		table.description("another test table");

		View view = new View();
		view.table(table);
		view.name("view1");
		view.description("a test view");
		view.keys("(day:int, hour:int), -minute:int");

		TestCallback<View> callback = new TestCallback<View>();

		// Create
		Futures.addCallback(views.create(view), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		// Document table should exist.
		assertTrue("View table not created: " + view.toDbTable(), tableExists(view.toDbTable()));

		// Read
		callback.clear();
		Futures.addCallback(views.read(view.identifier()), callback);
		waitFor(callback);

		assertEquals(view, callback.entity());

		// Update
		callback.clear();
		table.description("an updated test view");
		Futures.addCallback(views.update(view), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		Futures.addCallback(views.read(view.identifier()), callback);
		waitFor(callback);

		View result2 = callback.entity();
		assertEquals(view, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		waitFor(callback);
		TestCallback<Boolean> deleteCallback = new TestCallback<Boolean>();
		Futures.addCallback(views.delete(view.identifier()), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Document table should no longer exist.
		assertFalse("View table not deleted: " + view.toDbTable(), tableExists(view.toDbTable()));

		// Re-Read
		callback.clear();
		Futures.addCallback(views.read(view.identifier()), callback);
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

		View view = new View();
		view.table(table);
		view.name("v1");
		view.keys("(id:uuid)");
		View createResult = views.create(view).get();
		assertEquals(view, createResult);

		try
		{
			views.create(view).get();
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

		View view = new View();
		view.table(table);
		view.name("v1");
		view.keys("(id:uuid)");
		TestCallback<View> callback = new TestCallback<View>();

		// Create
		Futures.addCallback(views.create(view), callback);
		waitFor(callback);

		assertNotNull(callback.entity());
		assertNull(callback.throwable());

		// Create Duplicate
		Futures.addCallback(views.create(view), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldMarshal()
	throws InterruptedException, ExecutionException
	{
		// Create
		Table table = new Table();
		table.name("table5");
		table.database("db5");

		View view = new View();
		view.table(table);
		view.name("v1");
		view.keys("(id:uuid)");
		View createResult = views.create(view).get();
		assertEquals(view, createResult);

		View sync = views.read(view.identifier()).get();
		assertEquals(view.createdAt(), sync.createdAt());
		assertEquals(view.updatedAt(), sync.updatedAt());
		assertEquals(view.databaseName(), sync.databaseName());
		assertEquals(view.tableName(), sync.tableName());
		assertEquals(view.description(), sync.description());
		assertEquals(view.keys(), sync.keys());
		assertEquals(view.name(), sync.name());
		assertEquals(view.ttl(), sync.ttl());

		TestCallback<View> callback = new TestCallback<View>();
		Futures.addCallback(views.read(view.identifier()), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		View async = callback.entity();
		assertEquals(view.createdAt(), async.createdAt());
		assertEquals(view.updatedAt(), async.updatedAt());
		assertEquals(view.databaseName(), async.databaseName());
		assertEquals(view.tableName(), async.tableName());
		assertEquals(view.description(), async.description());
		assertEquals(view.keys(), async.keys());
		assertEquals(view.name(), async.name());
		assertEquals(view.ttl(), async.ttl());
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	throws Throwable
	{
		try
		{
			views.read(new Identifier("db5", "table5", "doesn't exist")).get();
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
		TestCallback<View> callback = new TestCallback<View>();
		Futures.addCallback(views.read(new Identifier("db6", "table6", "doesn't exist")), callback);
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
		table.name("tbl1");
		View view = new View();
		view.table(table);
		view.name("doesn't exist");
		view.keys("id:uuid");

		try
		{
			views.update(view).get();
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
		Table table = new Table();
		table.database("db8");
		table.name("tbl1");
		View view = new View();
		view.table(table);
		view.name("doesn't exist");
		view.keys("id:uuid");

		TestCallback<View> callback = new TestCallback<View>();
		Futures.addCallback(views.update(view), callback);
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
