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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author tfredrich
 * @since May 7, 2015
 */
public class KeyspaceSchema
implements Schemaable
{
	private class Schema
	{
		static final String DROP = "drop keyspace if exists %s";
		static final String CREATE = "create keyspace if not exists %s";
		static final String LOCAL_REPLICATION = " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";		
		// TODO: Make this configurable.
		static final String NETWORK_REPLICATION = " with replication = { 'class' : 'NetworkTopologyStrategy', 'use1' : 2, 'usw2' : 2}";
	}

	private boolean isNetworkReplication = true;

	public KeyspaceSchema()
    {
		super();
    }

	public void useLocalReplication()
	{
		isNetworkReplication = false;
	}

	@Override
	public boolean drop(Session session, String keyspace)
	{
		ResultSet rs = session.execute(String.format(Schema.DROP, keyspace));
		return rs.wasApplied();

	}

	@Override
	public boolean create(Session session, String keyspace)
	{
		String create = String.format(Schema.CREATE, keyspace);
		ResultSet rs;

		if (isNetworkReplication)
		{
			rs = session.execute(create + Schema.NETWORK_REPLICATION);
		}
		else
		{
			rs = session.execute(create + Schema.LOCAL_REPLICATION);
		}

		return rs.wasApplied();

	}
}
