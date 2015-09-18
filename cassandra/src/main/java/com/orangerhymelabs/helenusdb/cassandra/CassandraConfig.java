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

import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.orangerhymelabs.helenusdb.exception.ConfigurationException;

/**
 * @author tfredrich
 * @since May 7, 2015
 */
public class CassandraConfig
{
	private static final String DEFAULT_PORT = "9042";
	private static final String CONTACT_POINTS_PROPERTY = "cassandra.contactPoints";
	private static final String PORT_PROPERTY = "cassandra.port";
	private static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
	private static final String DATA_CENTER = "cassandra.dataCenter";
	private static final String READ_CONSISTENCY_LEVEL = "cassandra.readConsistencyLevel";
	private static final String WRITE_CONSISTENCY_LEVEL = "cassandra.writeConsistencyLevel";

	private String[] contactPoints;
	private String keyspace;
	private int port;
	private String dataCenter;
	private ConsistencyLevel readConsistencyLevel;
	private ConsistencyLevel writeConsistencyLevel;

	private Session session;
	private Session keyspaceSession;

	public CassandraConfig(Properties p)
	{
		port = Integer.parseInt(p.getProperty(PORT_PROPERTY, DEFAULT_PORT));
		dataCenter = p.getProperty(DATA_CENTER);
		readConsistencyLevel = ConsistencyLevel.valueOf(p.getProperty(READ_CONSISTENCY_LEVEL, "LOCAL_QUORUM"));
		writeConsistencyLevel = ConsistencyLevel.valueOf(p.getProperty(WRITE_CONSISTENCY_LEVEL, "LOCAL_QUORUM"));
		keyspace = p.getProperty(KEYSPACE_PROPERTY);

		if (keyspace == null || keyspace.trim().isEmpty())
		{
			throw new ConfigurationException(
			    "Please define a Cassandra keyspace in property: "
			        + KEYSPACE_PROPERTY);
		}

		String contactPointsCommaDelimited = p
		    .getProperty(CONTACT_POINTS_PROPERTY);

		if (contactPointsCommaDelimited == null
		    || contactPointsCommaDelimited.trim().isEmpty())
		{
			throw new ConfigurationException(
			    "Please define Cassandra contact points for property: "
			        + CONTACT_POINTS_PROPERTY);
		}

		contactPoints = contactPointsCommaDelimited.split(",\\s*");

		initialize(p);
	}

	/**
	 * Sub-classes can override to initialize other properties.
	 * 
	 * @param p Propreties
	 */
	protected void initialize(Properties p)
	{
		// default is to do nothing.
	}

	public String getKeyspace()
	{
		return keyspace;
	}

	public int getPort()
	{
		return port;
	}

	public String getDataCenter()
	{
		return dataCenter;
	}

	public ConsistencyLevel getReadConsistencyLevel()
	{
		return readConsistencyLevel;
	}

	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return writeConsistencyLevel;
	}

	public Session getKeyspaceSession()
	{
		if (keyspaceSession == null)
		{
			keyspaceSession = getCluster().connect(getKeyspace());
		}

		return keyspaceSession;
	}

	public Session getSession()
	{
		if (session == null)
		{
			session = getCluster().connect();
		}

		return session;
	}

	protected Cluster getCluster()
	{
		Builder cb = Cluster.builder();
		cb.addContactPoints(contactPoints);
		cb.withPort(getPort());

		if (getDataCenter() != null)
		{
			cb.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(getDataCenter()));
		}

		enrichCluster(cb);
		return cb.build();
	}

	/**
	 * Sub-classes override this method to do specialized cluster configuration.
	 * 
	 * @param clusterBuilder
	 */
	protected void enrichCluster(Builder clusterBuilder)
	{
		// default is to do nothing.
	}
}
