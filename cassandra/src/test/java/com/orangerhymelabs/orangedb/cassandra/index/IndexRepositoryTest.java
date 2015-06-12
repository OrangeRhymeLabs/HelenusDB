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
import static org.junit.Assert.assertFalse;
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

import com.datastax.driver.core.ResultSet;
import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.TestCallback;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class IndexRepositoryTest
{
	private static final int CALLBACK_TIMEOUT = 2000;

	private static KeyspaceSchema keyspace;
	private static IndexRepository indexes;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new IndexRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		indexes = new IndexRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
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
		index.table("db1", "table1", FieldType.UUID);
		index.description("a test index");
		index.fields(Arrays.asList("a:text", "b:timestamp", "-C:uuid"));
		index.isUnique(true);
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		// Underlying bucket table should exist.
		assertTrue("Bucket table not created: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Read
		Index result = indexes.read(index.getId());
		assertEquals(index, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		index.description("an updated test index");
		Index updateResult = indexes.update(index);
		assertEquals(index, updateResult);

		// Re-Read
		Index result2 = indexes.read(index.getId());
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		indexes.delete(index.getId());

		// Bucket table should no longer exist.
		assertFalse("Bucket table not deleted: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Re-Read table
		try
		{
			indexes.read(index.getId());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Index not deleted: " + index.getId().toString());
	}

	@Test
	public void shouldCRUDAsynchronously()
	throws InterruptedException
	{
		Index index = new Index();
		index.name("index1");
		index.table("db2", "table1", FieldType.UUID);
		index.description("another test index");
		index.fields(Arrays.asList("a:integer", "-b:DOUBLE", "-C:bigInt"));
		index.isUnique(true);
		TestCallback<Index> callback = new TestCallback<Index>();

		// Create
		indexes.createAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Underlying bucket table should exist.
		assertTrue("Bucket table not created: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Read
		callback.clear();
		indexes.readAsync(index.getId(), callback);
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
		indexes.readAsync(index.getId(), callback);
		waitFor(callback);

		Index result2 = callback.entity();
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		indexes.deleteAsync(index.getId(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Bucket table should no longer exist.
		assertFalse("Bucket table not deleted: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Re-Read
		callback.clear();
		indexes.readAsync(index.getId(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		Index index = new Index();
		index.name("index1");
		index.table("db3", "table1", FieldType.BIGINT);
		index.fields(Arrays.asList("foo:text"));
		index.isUnique(true);
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		indexes.create(index);
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		Index index = new Index();
		index.name("index2");
		index.table("db4", "table1", FieldType.TIMESTAMP);
		index.fields(Arrays.asList("foo:uuid", "-bar:integer", "bat:timestamp"));
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
	public void shouldThrowOnReadNonExistentSynchronously()
	{
		indexes.read(new Identifier("db5", "table5", "doesnt_exist"));
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		indexes.readAsync(new Identifier("db6", "table5", "doesnt_exist"), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	{
		Index index = new Index();
		index.name("index1");
		index.table("db1", "doesnt_exist", FieldType.UUID);
		indexes.update(index);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		Index index = new Index();
		index.name("index1");
		index.table("db1", "doesnt_exist", FieldType.UUID);
		indexes.updateAsync(index, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	private void waitFor(TestCallback<Index> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }

	private boolean tableExists(String tableName)
    {
	    ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system.schema_columnfamilies where keyspace_name='%s' and columnfamily_name='%s'", CassandraManager.keyspace(), tableName));
		return (rs.one().getLong(0) > 0);
    }
}
