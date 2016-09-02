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
package com.orangerhymelabs.helenus.cassandra.database;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository.DatabaseStatements;
import com.orangerhymelabs.helenus.persistence.Query;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DatabaseRepository
extends AbstractCassandraRepository<Database, DatabaseStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(DatabaseRepository.class);

	private class Tables
	{
		static final String BY_ID = "sys_db";
	}

	private class Columns
	{
		static final String NAME = "db_name";
		static final String DESCRIPTION = "description";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where " + Columns.NAME + " = ?";

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table if not exists %s." + Tables.BY_ID +
			"(" +
				Columns.NAME + " text," +
				Columns.DESCRIPTION + " text," +
				Columns.CREATED_AT + " timestamp," +
				Columns.UPDATED_AT + " timestamp," +
				"primary key (" + Columns.NAME + ")" +
			")";

		@Override
		public boolean drop(Session session, String keyspace)
		{
			ResultSetFuture rs = session.executeAsync(String.format(DROP_TABLE, keyspace));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Database schema drop failed", e);
			}

			return false;
		}

		@Override
		public boolean create(Session session, String keyspace)
		{
			ResultSetFuture rs = session.executeAsync(String.format(CREATE_TABLE, keyspace));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Database schema create failed", e);
			}

			return false;
		}
	}

	public interface DatabaseStatements
	extends StatementFactory
	{
		@Override
		@Query("insert into %s." + Tables.BY_ID + "(" + Columns.NAME + ", " + Columns.DESCRIPTION + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?) if not exists")
		PreparedStatement create();

		@Override
		@Query("update %s." + Tables.BY_ID + " set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists")
		PreparedStatement update();

		@Override
		@Query("delete from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement delete();

		@Query("select count(*) from %s." + Tables.BY_ID + IDENTITY_CQL + " limit 1")
		PreparedStatement exists();

		@Override
		@Query("select * from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement read();

		@Override
		@Query("select * from %s." + Tables.BY_ID)
		PreparedStatement readAll();
	}

	public DatabaseRepository(Session session, String keyspace)
	{
		super(session, keyspace, DatabaseStatements.class);
	}

	@Override
	protected void bindCreate(BoundStatement bs, Database entity)
	{
		Date now = new Date();
		entity.createdAt(now);
		entity.updatedAt(now);
		bs.bind(entity.name(),
			entity.description(),
			entity.createdAt(),
		    entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Database entity)
	{
		entity.updatedAt(new Date());
		bs.bind(entity.description(),
		    entity.updatedAt(),
		    entity.name());
	}

	protected Database marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Database n = new Database();
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.createdAt(row.getTimestamp(Columns.CREATED_AT));
		n.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return n;
	}
}
