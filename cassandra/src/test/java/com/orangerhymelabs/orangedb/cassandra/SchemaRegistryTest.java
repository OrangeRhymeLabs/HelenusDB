package com.orangerhymelabs.orangedb.cassandra;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;

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

	private boolean tableExists(String tableName)
    {
	    ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system.schema_columnfamilies where keyspace_name='%s' and columnfamily_name='%s'", CassandraManager.keyspace(), tableName));
		return (rs.one().getLong(0) > 0);
    }
}
