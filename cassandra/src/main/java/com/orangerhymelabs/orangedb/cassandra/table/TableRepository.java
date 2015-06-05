package com.orangerhymelabs.orangedb.cassandra.table;

import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.Schemaable;
import com.orangerhymelabs.orangedb.cassandra.document.DocumentRepository;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.exception.StorageException;
import com.orangerhymelabs.orangedb.persistence.Identifier;

public class TableRepository
extends AbstractCassandraRepository<Table>
{
	private class Tables
	{
		static final String BY_ID = "sys_tbl";
	}

	public static class Schema
	implements Schemaable
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				"db_name text," +
				"tbl_name text," +
				"description text," +
				"tbl_schema text," +
				"tbl_type int," +
				"tbl_ttl bigint," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((db_name), tbl_name)" +
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

	private class Columns
	{
		static final String NAME = "tbl_name";
		static final String DATABASE = "db_name";
		static final String DESCRIPTION = "description";
		static final String SCHEMA = "tbl_schema";
		static final String TYPE = "tbl_type";
		static final String TTL = "tbl_ttl";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.NAME + ", " + Columns.DATABASE + ", " + Columns.DESCRIPTION + ", " + Columns.SCHEMA + ", " + Columns.TYPE + ", " + Columns.TTL + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT +") values (?, ?, ?, ?, ?, ?, ?, ?) if not exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.DESCRIPTION + " = ?, " + Columns.SCHEMA + " = ?, " + Columns.TTL + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
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
			DOCUMENT_SCHEMA.create(session(), keyspace(), table.toDbTable());
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
			DOCUMENT_SCHEMA.create(session(), keyspace(), table.toDbTable());
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
		DOCUMENT_SCHEMA.drop(session(), keyspace(), Identifier.toSeparatedString(id, "_"));
		super.delete(id);
	}

	@Override
	public void deleteAsync(Identifier id, FutureCallback<Table> callback)
	{
		try
		{
			DOCUMENT_SCHEMA.drop(session(), keyspace(), Identifier.toSeparatedString(id, "_"));
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
	protected void bindCreate(BoundStatement bs, Table entity)
	{
		Date now = new Date();
		entity.createdAt(now);
		entity.updatedAt(now);
		bs.bind(entity.name(),
			entity.database().name(),
			entity.description(),
			entity.schema(),
			entity.type().ordinal(),
			entity.ttl(),
		    entity.createdAt(),
		    entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Table entity)
	{
		entity.updatedAt(new Date());
		bs.bind(entity.description(),
			entity.schema(),
			entity.ttl(),
			entity.updatedAt(),
			entity.database().name(),
			entity.name());
	}

	protected Table marshalRow(Row row)
	{
		if (row == null) return null;

		Table c = new Table();
		c.name(row.getString(Columns.NAME));
		c.database(row.getString(Columns.DATABASE));
		c.description(row.getString(Columns.DESCRIPTION));
		c.schema(row.getString(Columns.SCHEMA));
		c.ttl(row.getLong(Columns.TTL));
		c.type(TableType.valueOf(row.getInt(Columns.TYPE)));
		c.createdAt(row.getDate(Columns.CREATED_AT));
		c.updatedAt(row.getDate(Columns.UPDATED_AT));
		return c;
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
}
