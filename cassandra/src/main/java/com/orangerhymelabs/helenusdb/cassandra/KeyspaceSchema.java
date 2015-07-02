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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
		static final String NETWORK_REPLICATION = " with replication = { 'class' : 'NetworkTopologyStrategy', %s}";
	}

	private Map<String, Integer> dataCenters = null;

	public KeyspaceSchema()
    {
		super();
    }

	public boolean isNetworkReplication()
	{
		return (dataCenters != null);
	}

	public void useNetworkReplication(Map<String, Integer> dataCenters)
	{
		this.dataCenters = new HashMap<String, Integer>(dataCenters);
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

		if (isNetworkReplication())
		{
			rs = session.execute(create + String.format(Schema.NETWORK_REPLICATION, replicationFactors(dataCenters)));
		}
		else
		{
			rs = session.execute(create + Schema.LOCAL_REPLICATION);
		}

		return rs.wasApplied();

	}

	/**
	 * Creates a string of the form "'use1' : 2, 'usw2' : 2" that is used to set
	 * datacenter/replication factors on the NetworkTopologyStrategy.
	 * 
	 * @param replFactors a map of datacenter names with corresponding integer replication factors.
	 * @return a formatted string of the form, "'use1' : 2, 'usw2' : 2"
	 */
	String replicationFactors(Map<String, Integer> replFactors)
	{
		StringBuilder sb = new StringBuilder();
		boolean isFirst = true;

		for (Entry<String, Integer> entry : replFactors.entrySet())
		{
			if (isFirst)
			{
				sb.append("'");
			}
			else
			{
				sb.append(", '");
			}

			sb.append(entry.getKey());
			sb.append("' : ");
			sb.append(entry.getValue());
			isFirst = false;
		}

		return sb.toString();
	}
}
