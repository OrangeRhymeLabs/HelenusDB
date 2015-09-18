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
package com.orangerhymelabs.helenusdb.cassandra.meta;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenusdb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenusdb.cassandra.SchemaProvider;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class MetadataRepository
extends AbstractCassandraRepository<KeyValuePair>
{
	private class Tables
	{

		static final String BY_ID = "sys_meta";
	}

	private class Columns
	{
		static final String ID = "id";
		static final String VALUE = "value";
	}

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table if not exists %s." + Tables.BY_ID +
			"(" +
				Columns.ID + " text," +
				Columns.VALUE + " text," +
				"primary key (" + Columns.ID + ")" +
			")";

		@Override
		public boolean drop(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.DROP_TABLE, keyspace));
			return rs.wasApplied();
		}

		@Override
		public boolean create(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.CREATE_TABLE, keyspace));
			return rs.wasApplied();
		}
	}

	private static final String IDENTITY_CQL = " where " + Columns.ID + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.ID + ", " + Columns.VALUE + ") values (?, ?) if not exists";
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.VALUE + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;

	public MetadataRepository(Session session, String keyspace)
	{
		super(session, keyspace);
	}

	@Override
	protected String buildCreateStatement()
	{
		return String.format(CREATE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected String buildUpdateStatement()
	{
		return String.format(UPDATE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected String buildReadStatement()
	{
		return String.format(READ_CQL, keyspace(), Tables.BY_ID);
	}

	protected String buildReadAllStatement()
	{
		return String.format(READ_ALL_CQL, keyspace(), Tables.BY_ID);
	}

	protected String buildDeleteStatement()
	{
		return String.format(DELETE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected void bindCreate(BoundStatement bs, KeyValuePair entity)
	{
		bs.bind(entity.key(),
			entity.value());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, KeyValuePair entity)
	{
		bs.bind(entity.value(),
		    entity.key());
	}

	protected KeyValuePair marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		String id = row.getString(Columns.ID);
		String value = row.getString(Columns.VALUE);
		return new KeyValuePair(id, value);
	}
}
