package com.orangerhymelabs.orangedb.cassandra.table;

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

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.TestCallback;
import com.orangerhymelabs.orangedb.cassandra.database.Database;
import com.orangerhymelabs.orangedb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.orangedb.cassandra.database.DatabaseService;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationException;

public class TableServiceTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static TableService tables;
	private static DatabaseService databases;

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
		TableRepository tableRepository = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseRepository databaseRepository = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		tables = new TableService(databaseRepository, tableRepository);

		Database db = new Database();
		db.name("test_db");
		databaseRepository.create(db);
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCRUD()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.database("test_db");
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
		callback.clear();
		tables.delete(entity.getId(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		tables.read(entity.databaseName(), entity.name(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnDuplicate()
	throws InterruptedException
	{
		Table entity = new Table();
		entity.database("test_db");
		entity.name("table4");
		TestCallback<Table> callback = new TestCallback<Table>();

		// Create
		tables.create(entity, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		tables.create(entity, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldThrowOnReadNonExistent()
	throws InterruptedException
	{
		// Table doesn't exist
		TestCallback<Table> callback = new TestCallback<Table>();
		tables.read("test_db", "doesn't exist", callback);
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
	public void shouldThrowOnUpdateNonExistent()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database("test_db");
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
	public void shouldThrowOnInvalidDatabaseName()
	throws InterruptedException
	{
		TestCallback<Table> callback = new TestCallback<Table>();
		Table entity = new Table();
		entity.database("invalid db 8");
		entity.name("doesnt_matter");
		tables.update(entity, callback);
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
