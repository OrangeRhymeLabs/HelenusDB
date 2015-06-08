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
package com.orangerhymelabs.orangedb.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
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
		Map<String, Integer> dataCenters = new HashMap<String, Integer>();
		dataCenters.put("use1", 2);
		dataCenters.put("esw2", 2);

		KeyspaceSchema schema = new KeyspaceSchema();
		schema.useNetworkReplication(dataCenters);
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
	public void shouldCreateReplicationFactorString()
	{
		Map<String, Integer> dataCenters = new LinkedHashMap<String, Integer>();
		dataCenters.put("use1", 2);
		dataCenters.put("usw2", 3);

		KeyspaceSchema schema = new KeyspaceSchema();
		assertEquals("'use1' : 2, 'usw2' : 3", schema.replicationFactors(dataCenters));
	}
}
