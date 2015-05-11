package com.orangerhymelabs.orangedb.cassandra.table;

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
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

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
		keyspace.useLocalReplication();
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
		Table entity = new Table();
		entity.name("table1");
		entity.database("db1");
		entity.description("a test table");
		Table createResult = tables.create(entity);
		assertEquals(entity, createResult);

		// Read
		Table result = tables.read(entity.getId());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		entity.description("an updated test table");
		Table updateResult = tables.update(entity);
		assertEquals(entity, updateResult);

		// Re-Read
		Table result2 = tables.read(entity.getId());
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		tables.delete(entity.getId());

		// Re-Read
		try
		{
			tables.read(entity.getId());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Table not deleted: " + entity.getId().toString());
	}

	@Test
	public void shouldCreateAsynchronously()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.name("table2");
		entity.database("db2");
		entity.description("another test table");
		TableCallback callback = new TableCallback();

		// Create
		tables.createAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		tables.readAsync(entity.getId(), callback);
		waitFor(callback);

		assertEquals(entity, callback.table());

		// Update
		callback.clear();
		entity.description("an updated test table");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		tables.readAsync(entity.getId(), callback);
		waitFor(callback);

		Table result2 = callback.table();
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		tables.deleteAsync(entity.getId(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		tables.readAsync(entity.getId(), callback);
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
		entity.database("db3");
		Table createResult = tables.create(entity);
		assertEquals(entity, createResult);

		tables.create(entity);
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.name("table4");
		entity.database("db4");
		TableCallback callback = new TableCallback();

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
	public void shouldThrowOnReadNonExistentSynchronously()
	{
		tables.read(new Identifier("db5", "doesn't exist"));
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TableCallback callback = new TableCallback();
		tables.readAsync(new Identifier("db6", "doesn't exist"), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	{
		Table entity = new Table();
		entity.database("db7");
		entity.name("doesn't exist");
		tables.update(entity);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TableCallback callback = new TableCallback();
		Table entity = new Table();
		entity.database("db8");
		entity.name("doesn't exist");
		tables.updateAsync(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	private void waitFor(TableCallback callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }

	private class TableCallback
	implements ResultCallback<Table>
	{
		private Table table;
		private Throwable throwable;

		@Override
        public void onSuccess(Table result)
        {
			this.table = result;
			alert();
        }

		@Override
        public void onFailure(Throwable t)
        {
			this.throwable = t;
			alert();
        }

		private synchronized void alert()
		{
			notifyAll();
		}

		public void clear()
		{
			this.table = null;
			this.throwable = null;
		}

		public boolean isEmpty()
		{
			return (table == null && throwable == null);
		}

		public Table table()
		{
			return table;
		}

		public Throwable throwable()
		{
			return throwable;
		}
	}
}
