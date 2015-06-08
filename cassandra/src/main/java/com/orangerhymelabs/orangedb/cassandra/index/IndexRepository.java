package com.orangerhymelabs.orangedb.cassandra.index;

import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.Schemaable;

public class IndexRepository
extends AbstractCassandraRepository<Index>
{
	private class Tables
	{

		static final String BY_ID = "sys_idx";
	}

	public static class Schema
	implements Schemaable
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				"db_name text," +
				"tbl_name text," +
				"name text," +
				"description text," +
				"is_unique boolean," +
				"fields list<text>," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((db_name), tbl_name, name)" +
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

	private class Columns
	{
		static final String DB_NAME = "db_name";
		static final String TBL_NAME = "tbl_name";
		static final String NAME = "name";
		static final String DESCRIPTION = "description";
		static final String IS_UNIQUE = "is_unique";
		static final String FIELDS = "fields";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where " + Columns.DB_NAME + "= ? " + Columns.TBL_NAME + " = ? " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.DB_NAME + ", " + Columns.TBL_NAME + ", " + Columns.NAME + ", " + Columns.DESCRIPTION + ", " + Columns.FIELDS + ", " + Columns.IS_UNIQUE + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?, ?, ?, ?, ?) if not exists";
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;

	public IndexRepository(Session session, String keyspace)
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
	protected void bindCreate(BoundStatement bs, Index index)
	{
		Date now = new Date();
		index.createdAt(now);
		index.updatedAt(now);
		bs.bind(index.databaseName(),
			index.tableName(),
			index.name(),
			index.description(),
			index.fields(),
			index.isUnique(),
			index.createdAt(),
		    index.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Index index)
	{
		index.updatedAt(new Date());
		bs.bind(index.description(),
		    index.updatedAt(),
		    index.databaseName(),
		    index.tableName(),
		    index.name());
	}

	protected Index marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Index n = new Index();
		n.table(row.getString(Columns.DB_NAME), row.getString(Columns.TBL_NAME));
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.fields(row.getList(Columns.FIELDS, String.class));
		n.isUnique(row.getBool(Columns.IS_UNIQUE));
		n.createdAt(row.getDate(Columns.CREATED_AT));
		n.updatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
	}
}