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
package com.orangerhymelabs.helenus.cassandra.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.index.BucketedViewIndex;
import com.orangerhymelabs.helenus.cassandra.index.Index;
import com.orangerhymelabs.helenus.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenus.cassandra.index.LuceneIndex;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;

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
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db1", "table1", DataTypes.UUID);
		index.description("a test index");
		index.fields(Arrays.asList("a:text", "b:timestamp", "-C:uuid"));
		index.isUnique(true);
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		// Underlying bucket table should exist.
		assertTrue("Bucket table not created: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Read
		Index result = indexes.read(index.getIdentifier());
		assertTrue(result instanceof BucketedViewIndex);
		assertEquals(index, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		index.description("an updated test index");
		Index updateResult = indexes.update(index);
		assertEquals(index, updateResult);

		// Re-Read
		Index result2 = indexes.read(index.getIdentifier());
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		indexes.delete(index.getIdentifier());

		// Bucket table should no longer exist.
		assertFalse("Bucket table not deleted: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Re-Read table
		try
		{
			indexes.read(index.getIdentifier());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Index not deleted: " + index.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDLuceneIndexSynchronously()
	throws Exception
	{
		// Create
		LuceneIndex index = new LuceneIndex();
		index.name("lucene1");
		index.table("db1", "table1", DataTypes.UUID);
		index.description("a test Lucene index");
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		// Lucene doesn't require underlying bucket table.
		assertFalse("Should not create bucket table: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Read
		Index result = indexes.read(index.getIdentifier());
		assertTrue(result instanceof LuceneIndex);
		assertEquals(index, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		index.description("an updated test Lucene index");
		Index updateResult = indexes.update(index);
		assertEquals(index, updateResult);

		// Re-Read
		Index result2 = indexes.read(index.getIdentifier());
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		indexes.delete(index.getIdentifier());

		// Re-Read table
		try
		{
			indexes.read(index.getIdentifier());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Index not deleted: " + index.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDAsynchronously()
	throws InterruptedException
	{
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db2", "table1", DataTypes.UUID);
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
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.entity() instanceof BucketedViewIndex);
		assertEquals(index, callback.entity());

		// Update
		callback.clear();
		index.description("an updated test index");
		indexes.updateAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		Index result2 = callback.entity();
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		indexes.deleteAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Bucket table should no longer exist.
		assertFalse("Bucket table not deleted: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Re-Read
		callback.clear();
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldCRUDLuceneIndexAsynchronously()
	throws InterruptedException
	{
		LuceneIndex index = new LuceneIndex();
		index.name("lucene1");
		index.table("db2", "table1", DataTypes.UUID);
		index.description("another test Lucene index");
		TestCallback<Index> callback = new TestCallback<Index>();

		// Create
		indexes.createAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Lucene doesn't require underlying bucket table.
		assertFalse("Should not create bucket table: " + index.toDbTable(), tableExists(index.toDbTable()));

		// Read
		callback.clear();
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.entity() instanceof LuceneIndex);
		assertEquals(index, callback.entity());

		// Update
		callback.clear();
		index.description("an updated test Lucene index");
		indexes.updateAsync(index, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		Index result2 = callback.entity();
		assertEquals(index, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		indexes.deleteAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		indexes.readAsync(index.getIdentifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db3", "table1", DataTypes.BIGINT);
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
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index2");
		index.table("db4", "table1", DataTypes.TIMESTAMP);
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

	@Test
	public void shouldReadForSynchronously()
	{
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db6", "table1", DataTypes.BIGINT);
		index.fields(Arrays.asList("foo:text"));
		index.isUnique(true);
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		index.name("index2");
		index.fields(Arrays.asList("foo:text", "bar:text"));
		indexes.create(index);
		assertEquals(index, createResult);

		index.name("index3");
		index.fields(Arrays.asList("foo:text", "bar:text", "bat:timestamp"));
		indexes.create(index);
		assertEquals(index, createResult);

		List<Index> list = indexes.readFor("db6", "table1");
		assertNotNull(list);
		assertEquals(3, list.size());
		assertEquals("index1", list.get(0).name());
		assertEquals("index2", list.get(1).name());
		assertEquals("index3", list.get(2).name());
	}

	@Test
	public void shouldReadForAynchronously()
	throws InterruptedException
	{
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db6", "table2", DataTypes.BIGINT);
		index.fields(Arrays.asList("foo:text"));
		index.isUnique(true);
		Index createResult = indexes.create(index);
		assertEquals(index, createResult);

		index.name("index2");
		index.fields(Arrays.asList("foo:text", "bar:text"));
		indexes.create(index);
		assertEquals(index, createResult);

		index.name("index3");
		index.fields(Arrays.asList("foo:text", "-bar:text", "bat:timestamp"));
		indexes.create(index);
		assertEquals(index, createResult);

		TestCallback<List<Index>> callback = new TestCallback<List<Index>>();
		indexes.readForAsync("db6", "table2", callback);

		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}

		List<Index> list = callback.entity();
		assertNotNull(list);
		assertEquals(3, list.size());
		assertEquals("index1", list.get(0).name());
		assertEquals("index2", list.get(1).name());
		assertEquals("index3", list.get(2).name());
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
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db1", "doesnt_exist", DataTypes.UUID);
		indexes.update(index);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Index> callback = new TestCallback<Index>();
		BucketedViewIndex index = new BucketedViewIndex();
		index.name("index1");
		index.table("db1", "doesnt_exist", DataTypes.UUID);
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
	    ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system_schema.tables where keyspace_name='%s' and table_name='%s'", CassandraManager.keyspace(), tableName));
		return (rs.one().getLong(0) > 0);
    }
}
