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
package com.orangerhymelabs.helenus.cassandra.table;

import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.document.DocumentRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository.TableStatements;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionParser;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.Query;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableRepository
extends AbstractCassandraRepository<Table, TableStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(TableRepository.class);
	private static final KeyDefinitionParser KEY_PARSER = new KeyDefinitionParser();

	private class Tables
	{
		static final String BY_ID = "sys_tbl";
	}

	private class Columns
	{
		static final String NAME = "tbl_name";
		static final String DATABASE = "db_name";
		static final String DESCRIPTION = "description";
		static final String TYPE = "tbl_type";
		static final String KEYS = "keys";
		static final String TTL = "tbl_ttl";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				Columns.DATABASE + " text," +
				Columns.NAME + " text," +
				Columns.DESCRIPTION + " text," +
				Columns.TYPE + " text," +
				Columns.KEYS + " text," +
				Columns.TTL + " bigint," +
				Columns.CREATED_AT + " timestamp," +
				Columns.UPDATED_AT + " timestamp," +
				"primary key ((" + Columns.DATABASE + "), " + Columns.NAME + ")" +
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
		    	LOG.error("Table schema drop failed", e);
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
		    	LOG.error("Table schema create failed", e);
			}

		    return false;
	    }
	}

	private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";

	public interface TableStatements
	extends StatementFactory
	{
		@Override
		@Query("insert into %s." + Tables.BY_ID + " ("
		+ Columns.NAME + ", "
		+ Columns.DATABASE + ", "
		+ Columns.DESCRIPTION + ", "
		+ Columns.TYPE + ", "
		+ Columns.KEYS + ", "
		+ Columns.TTL + ", "
		+ Columns.CREATED_AT + ", "
		+ Columns.UPDATED_AT
		+") values (?, ?, ?, ?, ?, ?, ?, ?) if not exists")
		PreparedStatement create();

		@Override
		@Query("delete from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement delete();

		@Override
		@Query("update %s." + Tables.BY_ID + " set " + Columns.DESCRIPTION + " = ?, " + Columns.TTL + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists")
		PreparedStatement update();

		@Override
		@Query("select * from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement read();

		@Override
		@Query("select count(*) from %s." + Tables.BY_ID + IDENTITY_CQL + " limit 1")
		PreparedStatement exists();

		@Override
		@Query("select * from %s." + Tables.BY_ID + " where " + Columns.DATABASE + " = ?")
		PreparedStatement readAll();
	}

	private static final  DocumentRepository.Schema DOCUMENT_SCHEMA = new DocumentRepository.Schema();

	public TableRepository(Session session, String keyspace)
	{
		super(session, keyspace, TableStatements.class);
	}

	@Override
	public ListenableFuture<Table> create(Table table)
	{
		if (createDocumentSchema(table))
		{
			// TODO: what about rollback?
			return super.create(table);
		}
		else
		{
			return Futures.immediateFailedFuture(new StorageException("Failed to create document schema for: " + table.toDbTable()));
		}
	}

	@Override
	public ListenableFuture<Boolean> delete(Identifier id)
	{
		if (dropDocumentSchema(id))
		{
			// TODO: what about rollback?
			return super.delete(id);
		}
		else
		{
			return Futures.immediateFailedFuture(new StorageException("Failed to drop document schema for: " + id.toDbName()));
		}
	}

	@Override
	protected void bindCreate(BoundStatement bs, Table table)
	{
		Date now = new Date();
		table.createdAt(now);
		table.updatedAt(now);
		bs.bind(table.name(),
			table.database().name(),
			table.description(),
			table.type().name(),
			table.keys(),
			table.ttl(),
		    table.createdAt(),
		    table.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Table table)
	{
		table.updatedAt(new Date());
		bs.bind(table.description(),
			table.ttl(),
			table.updatedAt(),
			table.database().name(),
			table.name());
	}

	protected Table marshalRow(Row row)
	{
		if (row == null) return null;

		Table t = new Table();
		t.name(row.getString(Columns.NAME));
		t.database(row.getString(Columns.DATABASE));
		t.description(row.getString(Columns.DESCRIPTION));
		t.ttl(row.getLong(Columns.TTL));
		t.type(TableType.from(row.getString(Columns.TYPE)));
		t.keys(row.getString(Columns.KEYS));
		t.createdAt(row.getTimestamp(Columns.CREATED_AT));
		t.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return t;
	}

	private boolean createDocumentSchema(Table table)
    {
		try
		{
			return DOCUMENT_SCHEMA.create(session(), keyspace(), table.toDbTable(), KEY_PARSER.parse(table.keys()));
		}
		catch (KeyDefinitionException e)
		{
			throw new StorageException(e);
		}
    }

	private boolean dropDocumentSchema(Identifier id)
    {
		return DOCUMENT_SCHEMA.drop(session(), keyspace(), id.toDbName());
    }
}
