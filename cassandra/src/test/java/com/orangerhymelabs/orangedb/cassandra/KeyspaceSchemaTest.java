package com.orangerhymelabs.orangedb.cassandra;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class KeyspaceSchemaTest
{
	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
	}

	@Test
	public void shouldUseLocalReplication()
	{
		KeyspaceSchema schema = new KeyspaceSchema();
		schema.useLocalReplication();
		assertTrue(schema.create(CassandraManager.session(), CassandraManager.keyspace()));
		Session session = CassandraManager.cluster().connect(CassandraManager.keyspace());
		assertNotNull(session);
		assertTrue(schema.drop(CassandraManager.session(), CassandraManager.keyspace()));
		try
		{
			CassandraManager.cluster().connect(CassandraManager.keyspace());
		}
		catch(InvalidQueryException e)
		{
			return;
		}

		fail("Did not drop the keyspace: " + CassandraManager.keyspace());
	}

	@Test
	public void shouldUseNetworkReplication()
	{
		KeyspaceSchema schema = new KeyspaceSchema();
		assertTrue(schema.create(CassandraManager.session(), CassandraManager.keyspace()));
		Session session = CassandraManager.cluster().connect(CassandraManager.keyspace());
		assertNotNull(session);
		assertTrue(schema.drop(CassandraManager.session(), CassandraManager.keyspace()));
		try
		{
			CassandraManager.cluster().connect(CassandraManager.keyspace());
		}
		catch(InvalidQueryException e)
		{
			return;
		}

		fail("Did not drop the keyspace: " + CassandraManager.keyspace());
	}
}
