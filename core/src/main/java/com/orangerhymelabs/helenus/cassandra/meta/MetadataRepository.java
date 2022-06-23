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
package com.orangerhymelabs.helenus.cassandra.meta;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.meta.MetadataRepository.MetaStatements;
import com.orangerhymelabs.helenus.persistence.Query;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class MetadataRepository
extends AbstractCassandraRepository<KeyValuePair, MetaStatements>
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

	public interface MetaStatements
	extends StatementFactory
	{
		@Override
		@Query("insert into %s." + Tables.BY_ID + " (" + Columns.ID + ", " + Columns.VALUE + ") values (?, ?) if not exists")
		PreparedStatement create();

		@Override
		@Query("delete from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement delete();

		@Override
		@Query("update %s." + Tables.BY_ID + " set " + Columns.VALUE + " = ?" + IDENTITY_CQL + " if exists")
		PreparedStatement update();

		@Override
		@Query("select * from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement read();

		@Override
		@Query("select * from %s." + Tables.BY_ID)
		PreparedStatement readAll();
	}

	public MetadataRepository(Session session, String keyspace)
	{
		super(session, keyspace, MetaStatements.class);
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
