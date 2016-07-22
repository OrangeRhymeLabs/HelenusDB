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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.document.DocumentRepository;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableRepository
extends AbstractCassandraRepository<Table>
{
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
		static final String TTL = "tbl_ttl";
		static final String ID_TYPE = "id_type";
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
				Columns.TTL + " bigint," +
				Columns.ID_TYPE + " text," +
				Columns.CREATED_AT + " timestamp," +
				Columns.UPDATED_AT + " timestamp," +
				"primary key ((" + Columns.DATABASE + "), " + Columns.NAME + ")" +
			")";

		@Override
	    public boolean drop(Session session, String keyspace)
	    {
			ResultSet rs = session.execute(String.format(DROP_TABLE, keyspace));
		    return rs.wasApplied();
	    }

		@Override
	    public boolean create(Session session, String keyspace)
	    {
			ResultSet rs = session.execute(String.format(CREATE_TABLE, keyspace));
		    return rs.wasApplied();
	    }
	}

	private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.NAME + ", " + Columns.DATABASE + ", " + Columns.DESCRIPTION + ", " + Columns.TYPE + ", " + Columns.TTL + ", " + Columns.ID_TYPE + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT +") values (?, ?, ?, ?, ?, ?, ?, ?) if not exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.DESCRIPTION + " = ?, " + Columns.TTL + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_ALL_CQL = "select * from %s.%s where " + Columns.DATABASE + " = ?";

	private static final  DocumentRepository.Schema DOCUMENT_SCHEMA = new DocumentRepository.Schema();

	public TableRepository(Session session, String keyspace)
	{
		super(session, keyspace);
	}

	@Override
	public void createAsync(Table table, FutureCallback<Table> callback)
	{
		try
		{
			createDocumentSchema(table);
			super.createAsync(table, callback);
		}
		catch(AlreadyExistsException e)
		{
			callback.onFailure(new DuplicateItemException(e));
		}
		catch(Exception e)
		{
			callback.onFailure(new StorageException(e));
		}
	}

	@Override
	public Table create(Table table)
	{
		try
		{
			createDocumentSchema(table);
			return super.create(table);
		}
		catch(AlreadyExistsException e)
		{
			throw new DuplicateItemException(e);
		}
	}

	@Override
	public void delete(Identifier id)
	{
		dropDocumentSchema(id);
		super.delete(id);
	}

	@Override
	public void deleteAsync(Identifier id, FutureCallback<Table> callback)
	{
		try
		{
			dropDocumentSchema(id);
			super.deleteAsync(id, callback);
		}
		catch(AlreadyExistsException e)
		{
			callback.onFailure(new ItemNotFoundException(e));
		}
		catch(Exception e)
		{
			callback.onFailure(new StorageException(e));
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
			table.ttl(),
			table.idType().name(),
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
		t.idType(DataTypes.from(row.getString(Columns.ID_TYPE)));
		t.createdAt(row.getTimestamp(Columns.CREATED_AT));
		t.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return t;
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

	@Override
    protected String buildReadAllStatement()
    {
	    return String.format(READ_ALL_CQL, keyspace(), Tables.BY_ID);
    }

	@Override
    protected String buildDeleteStatement()
    {
	    return String.format(DELETE_CQL, keyspace(), Tables.BY_ID);
    }

	private void createDocumentSchema(Table table)
    {
    	DOCUMENT_SCHEMA.create(session(), keyspace(), table.toDbTable(), table.idType());
    }

	private void dropDocumentSchema(Identifier id)
    {
		DOCUMENT_SCHEMA.drop(session(), keyspace(), id.toDbName());
    }
}
