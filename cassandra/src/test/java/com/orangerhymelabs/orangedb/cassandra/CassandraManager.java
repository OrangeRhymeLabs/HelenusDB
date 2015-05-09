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

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * @author toddf
 * @since May 8, 2015
 */
public class CassandraManager
{
	private static final CassandraManager INSTANCE = new CassandraManager();
	private static final String LOCALHOST = "127.0.0.1";
	private static final String KEYSPACE_NAME = "orangedb_test";

	private boolean isStarted;
	private Cluster cluster;
	private Session session;
	private Metadata metadata;

	public static void start()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		INSTANCE._start();
	}

	public static Cluster cluster()
    {
	    return INSTANCE._cluster();
    }

	public static Session session()
	{
		return INSTANCE._session();
	}

	public static String keyspace()
	{
		return KEYSPACE_NAME;
	}

	public static Metadata metadata()
	{
		return INSTANCE._metadata();
	}

	private Cluster _cluster()
	{
		if (isStarted)
		{
			return cluster;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing cluster");		
	}

	private Session _session()
	{
		if (isStarted)
		{
			return session;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing session");
	}

	private Metadata _metadata()
	{
		if (isStarted)
		{
			return metadata;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing metadata");
	}

	private void _start()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		if (isStarted) return;

		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		cluster = Cluster.builder()
			.addContactPoints(LOCALHOST)
			.withPort(9142)
			.build();
		session = cluster.connect();
		metadata = cluster.getMetadata();
		isStarted = true;
	}
}
