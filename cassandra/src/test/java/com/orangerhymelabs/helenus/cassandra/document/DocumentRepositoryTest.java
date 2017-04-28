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
package com.orangerhymelabs.helenus.cassandra.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.bson.BSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.mongodb.util.JSON;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 16, 2015
 */
public class DocumentRepositoryTest
{
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final BSONObject BSON = (BSONObject) JSON.parse("{'a':'some', 'b':1, 'c':'excitement'}");

	private static KeyspaceSchema keyspace;
	private static AbstractDocumentRepository uuidDocs;
	private static AbstractDocumentRepository dateDocs;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException, ExecutionException, KeyDefinitionException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());

		new TableRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		TableRepository tables = new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());

		DocumentRepositoryFactory factory = new DocumentRepositoryFactoryImpl(CassandraManager.session(), CassandraManager.keyspace());

		Table uuids = new Table();
		uuids.name("uuids");
		uuids.database("db1");
		uuids.description("a test UUID-keyed table");
		Table uuidTable = tables.create(uuids).get();
		uuidDocs = factory.newInstance(uuidTable);

		Table dates = new Table();
		dates.name("dates");
		dates.database("db1");
		dates.keys("id:timestamp");
		dates.description("a test date-keyed table");
		Table dateTable = tables.create(dates).get();
		dateDocs = factory.newInstance(dateTable);
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
		doc.identifier(new Identifier(id));
		Document createResult = uuidDocs.create(doc).get();

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = uuidDocs.read(doc.identifier()).get();
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = uuidDocs.update(doc).get();
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = uuidDocs.read(doc.identifier()).get();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		uuidDocs.delete(doc.identifier()).get();

		// Re-Read
		try
		{
			uuidDocs.read(doc.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (e.getCause() instanceof ItemNotFoundException) return;
		}

		fail("Document not deleted: " + doc.identifier().toString());
	}

	@Test
	public void shouldCRUDAltIdSynchronously()
	throws Exception
	{
		// Create
		Date now = new Date();
		Document doc = new Document();
		doc.identifier(new Identifier(now));
		Document createResult = dateDocs.create(doc).get();

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = dateDocs.read(doc.identifier()).get();
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = dateDocs.update(doc).get();
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = dateDocs.read(doc.identifier()).get();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		dateDocs.delete(doc.identifier()).get();

		// Re-Read
		try
		{
			dateDocs.read(doc.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (e.getCause() instanceof ItemNotFoundException) return;
		}

		fail("Document not deleted: " + doc.identifier().toString());
	}

	@Test
	public void shouldCRUDUUIDAsynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		Futures.addCallback(uuidDocs.create(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		Futures.addCallback(uuidDocs.read(doc.identifier()), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		Futures.addCallback(uuidDocs.update(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		Futures.addCallback(uuidDocs.read(doc.identifier()), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<>();
		Futures.addCallback(uuidDocs.delete(doc.identifier()), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		Futures.addCallback(uuidDocs.read(doc.identifier()), callback);
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
		doc.identifier(new Identifier(now));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		Futures.addCallback(dateDocs.create(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		Futures.addCallback(dateDocs.read(doc.identifier()), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		Futures.addCallback(dateDocs.update(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		Futures.addCallback(dateDocs.read(doc.identifier()), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<>();
		Futures.addCallback(dateDocs.delete(doc.identifier()), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		Futures.addCallback(dateDocs.read(doc.identifier()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	throws Throwable
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		Document createResult = uuidDocs.create(doc).get();
		assertEquals(doc, createResult);

		try
		{
			uuidDocs.create(doc).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldReturnExistsBoolean()
	throws InterruptedException
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));

		TestCallback<Document> callback = new TestCallback<Document>();
		Futures.addCallback(uuidDocs.create(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		TestCallback<Boolean> existsCallback = new TestCallback<>();
		Futures.addCallback(uuidDocs.exists(new Identifier(id)), existsCallback);
		waitFor(existsCallback);
		assertTrue(existsCallback.entity());

		existsCallback.clear();
		Futures.addCallback(uuidDocs.exists(new Identifier(UUID.randomUUID())), existsCallback);
		waitFor(existsCallback);
		assertFalse(existsCallback.entity());
	}

	@Test
	public void shouldUpsert()
	throws InterruptedException
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));

		TestCallback<Document> callback = new TestCallback<Document>();
		Futures.addCallback(uuidDocs.upsert(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		callback.clear();
		Futures.addCallback(uuidDocs.upsert(doc), callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		TestCallback<Boolean> existsCallback = new TestCallback<>();
		Futures.addCallback(uuidDocs.exists(new Identifier(id)), existsCallback);
		waitFor(existsCallback);
		assertTrue(existsCallback.entity());
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		Futures.addCallback(uuidDocs.create(doc), callback);
		waitFor(callback);

		assertNotNull(callback.entity());
		assertNull(callback.throwable());

		// Create Duplicate
		Futures.addCallback(uuidDocs.create(doc), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnCreateInvalidIdSynchronously()
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		dateDocs.create(doc);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnCreateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		Futures.addCallback(dateDocs.create(doc), callback);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnUpdateInvalidIdSynchronously()
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		dateDocs.update(doc);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnUpdateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		Futures.addCallback(dateDocs.update(doc), callback);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	throws Throwable
	{
		try
		{
			uuidDocs.read(new Identifier(UUID.randomUUID())).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentAltIdSynchronously()
	throws Throwable
	{
		try
		{
			dateDocs.read(new Identifier(new Date())).get();
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
		TestCallback<Document> callback = new TestCallback<Document>();
		Futures.addCallback(uuidDocs.read(new Identifier(UUID.randomUUID())), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnReadNonExistentAltIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Futures.addCallback(dateDocs.read(new Identifier(new Date())), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	throws Throwable
	{
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));
		try
		{
			uuidDocs.update(doc).get();
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
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));
		Futures.addCallback(uuidDocs.update(doc), callback);
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
}
