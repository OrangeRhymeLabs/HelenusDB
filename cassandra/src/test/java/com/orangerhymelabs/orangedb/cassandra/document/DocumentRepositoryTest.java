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
package com.orangerhymelabs.orangedb.cassandra.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.bson.BSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.util.JSON;
import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.TestCallback;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 16, 2015
 */
public class DocumentRepositoryTest
{
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final BSONObject BSON = (BSONObject) JSON.parse("{'a':'some', 'b':1, 'c':'excitement'}");

	private static KeyspaceSchema keyspace;
	private static DocumentRepository uuidDocs;
	private static DocumentRepository dateDocs;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());

		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		TableRepository tables = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());

		Table uuids = new Table();
		uuids.name("uuids");
		uuids.database("db1");
		uuids.description("a test UUID-keyed table");
		Table uuidTable = tables.create(uuids);
		uuidDocs = new DocumentRepository(CassandraManager.session(), CassandraManager.keyspace(), uuidTable);

		Table dates = new Table();
		dates.name("dates");
		dates.database("db1");
		dates.idType(FieldType.TIMESTAMP);
		dates.description("a test date-keyed table");
		Table dateTable = tables.create(dates);
		dateDocs = new DocumentRepository(CassandraManager.session(), CassandraManager.keyspace(), dateTable);
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCRUDUUIDsSynchronously()
	throws Exception
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
//		doc.object(bson);
		Document createResult = uuidDocs.create(doc);

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = uuidDocs.read(doc.getIdentifier());
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(null);
		Document updateResult = uuidDocs.update(doc);
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = uuidDocs.read(doc.getIdentifier());
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		uuidDocs.delete(doc.getIdentifier());

		// Re-Read
		try
		{
			uuidDocs.read(doc.getIdentifier());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Document not deleted: " + doc.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDAltIdSynchronously()
	throws Exception
	{
		// Create
		Date now = new Date();
		Document doc = new Document();
		doc.id(now);
		Document createResult = dateDocs.create(doc);

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = dateDocs.read(doc.getIdentifier());
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = dateDocs.update(doc);
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = dateDocs.read(doc.getIdentifier());
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		dateDocs.delete(doc.getIdentifier());

		// Re-Read
		try
		{
			dateDocs.read(doc.getIdentifier());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Document not deleted: " + doc.getIdentifier().toString());
	}

	@Test
	public void shouldCRUDUUIDAsynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		uuidDocs.createAsync(doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		uuidDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		uuidDocs.updateAsync(doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		uuidDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		uuidDocs.deleteAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		uuidDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldCRUDAltIdAsynchronously()
	throws InterruptedException
	{
		Date now = new Date();
		Document doc = new Document();
		doc.id(now);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		dateDocs.createAsync(doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		dateDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		dateDocs.updateAsync(doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		dateDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		dateDocs.deleteAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		dateDocs.readAsync(doc.getIdentifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		Document createResult = uuidDocs.create(doc);
		assertEquals(doc, createResult);

		uuidDocs.create(doc);
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		uuidDocs.createAsync(doc, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		uuidDocs.createAsync(doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	{
		uuidDocs.read(new Identifier(UUID.randomUUID()));
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentAltIdSynchronously()
	{
		dateDocs.read(new Identifier(new Date()));
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		uuidDocs.readAsync(new Identifier(UUID.randomUUID()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnReadNonExistentAltIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		dateDocs.readAsync(new Identifier(new Date()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	{
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		uuidDocs.update(doc);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		uuidDocs.updateAsync(doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	private void waitFor(TestCallback<Document> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
