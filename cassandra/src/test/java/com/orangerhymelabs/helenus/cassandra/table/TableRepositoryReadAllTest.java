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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableRepositoryReadAllTest
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
	public void shouldReadAll()
	throws InterruptedException
	{
		shouldReturnEmptyListSynchronously();
		shouldReturnEmptyListAsynchronously();
		populateDatabase();
		shouldReturnListSynchronously();
		shouldReturnListAsynchronously();
	}

	private void shouldReturnEmptyListSynchronously()
    {
		List<Table> tbs = tables.readAll("database1");
		assertNotNull(tbs);
		assertTrue(tbs.isEmpty());
    }

	private void shouldReturnEmptyListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<Table>> callback = new TestCallback<List<Table>>();
		tables.readAllAsync(callback, "database1");
		waitFor(callback);

		assertFalse(callback.isEmpty());
		assertTrue(callback.entity().isEmpty());
    }

	private void populateDatabase()
    {
		Table table = new Table();
		table.database("database1");
		table.name("table1");
		tables.create(table);
		table.name("table2");
		tables.create(table);
		table.name("table3");
		tables.create(table);
		table.name("table4");
		tables.create(table);

		table.database("database2");
		table.name("table1");
		tables.create(table);
    }

	private void shouldReturnListSynchronously()
    {
		List<Table> entities = tables.readAll("database1");
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<Table>> callback = new TestCallback<List<Table>>();
		tables.readAllAsync(callback, "database1");
		waitFor(callback);

		List<Table> entities = callback.entity();
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void waitFor(TestCallback<List<Table>> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
