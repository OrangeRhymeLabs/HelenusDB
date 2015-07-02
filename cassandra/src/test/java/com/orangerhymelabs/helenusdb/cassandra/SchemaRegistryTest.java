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
package com.orangerhymelabs.helenusdb.cassandra;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.orangerhymelabs.helenusdb.cassandra.SchemaRegistry;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class SchemaRegistryTest
{
	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		SchemaRegistry.instance().initializeAll(CassandraManager.session(), CassandraManager.keyspace());
	}

	@AfterClass
	public static void afterClass()
	{
		SchemaRegistry.instance().dropAll(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void keyspaceShouldExist()
	{
		ResultSet rs = CassandraManager.session().execute(String.format("SELECT count(*) FROM system.schema_keyspaces where keyspace_name='%s'", CassandraManager.keyspace()));
		assertTrue("Keyspace not created: " + CassandraManager.keyspace(), rs.one().getLong(0) > 0);
	}

	@Test
	public void databaseTableShouldExist()
	{
		assertTrue("Table not created: sys_db", tableExists("sys_db"));
	}

	@Test
	public void tableTableShouldExist()
	{
		assertTrue("Table not created: sys_tbl", tableExists("sys_tbl"));
	}

	@Test
	public void indexTableShouldExist()
	{
		assertTrue("Table not created: sys_idx", tableExists("sys_idx"));
	}

	private boolean tableExists(String tableName)
    {
	    ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system.schema_columnfamilies where keyspace_name='%s' and columnfamily_name='%s'", CassandraManager.keyspace(), tableName));
		return (rs.one().getLong(0) > 0);
    }
}
