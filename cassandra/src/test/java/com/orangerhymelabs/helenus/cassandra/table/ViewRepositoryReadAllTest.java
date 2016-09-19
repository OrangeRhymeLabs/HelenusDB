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
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.View;
import com.orangerhymelabs.helenus.cassandra.table.ViewRepository;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ViewRepositoryReadAllTest
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
	public void shouldReadAll()
	throws Throwable
	{
		shouldReturnEmptyListSynchronously();
		shouldReturnEmptyListAsynchronously();
		populateDatabase();
		shouldReturnListSynchronously();
		shouldReturnListAsynchronously();
	}

	private void shouldReturnEmptyListSynchronously()
	throws Throwable
    {
		List<View> tbs = views.readAll("database1").get();
		assertNotNull(tbs);
		assertTrue(tbs.isEmpty());
    }

	private void shouldReturnEmptyListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<View>> callback = new TestCallback<List<View>>();
		Futures.addCallback(views.readAll("database1"), callback);
		waitFor(callback);

		assertFalse(callback.isEmpty());
		assertTrue(callback.entity().isEmpty());
    }

	private void populateDatabase()
    {
		Table table = new Table();
		table.database("database1");
		table.name("table1");
		
		View view = new View();
		view.table(table);
		view.keys("id:uuid");
		view.name("view1");
		views.create(view);
		view.name("view2");
		views.create(view);
		view.name("view3");
		views.create(view);
		view.name("view4");
		views.create(view);

		table.database("database2");
		table.name("table1");
		view.table(table);
		views.create(view);
    }

	private void shouldReturnListSynchronously()
	throws InterruptedException, ExecutionException
    {
		List<View> entities = views.readAll("database1").get();
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		TestCallback<List<View>> callback = new TestCallback<List<View>>();
		Futures.addCallback(views.readAll("database1"), callback);
		waitFor(callback);

		List<View> entities = callback.entity();
		assertNotNull(entities);
		assertFalse(entities.isEmpty());
		assertEquals(4, entities.size());
    }

	private void waitFor(TestCallback<List<View>> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
