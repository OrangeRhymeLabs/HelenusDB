package com.orangerhymelabs.orangedb.cassandra.table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.event.EventFactory;
import com.orangerhymelabs.orangedb.cassandra.event.StateChangeEventingObserver;
import com.orangerhymelabs.orangedb.persistence.Identifier;

public class TableRepository
extends AbstractCassandraRepository<Table>
{
	private class Tables
	{
		static final String BY_ID = "sys_tbl";
	}

	private class Schema
	{
		static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				"db_name text," +
				"tbl_name text," +
				"description text," +
				"tbl_schema text," +
				"tbl_type int," +
				"tbl_ttl bigint," +
				"hist_ttl bigint," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((db_name, tbl_name), updated_at)" +
			")" +
			"with clustering order by (updated_at DESC)";
	}

	private class Columns
	{
		static final String NAME = "tbl_name";
		static final String DATABASE = "db_name";
		static final String DESCRIPTION = "description";
		static final String SCHEMA = "tbl_schema";
		static final String TYPE = "tbl_type";
		static final String TTL = "tbl_ttl";
		static final String HISTORY_TTL = "history_ttl";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where db_name = ? and tbl_name = ?";
	private static final String EXISTENCE_CQL = "select count(*) from %s.%s" + IDENTITY_CQL;
	private static final String CREATE_CQL = "insert into %s.%s (tbl_name, db_name, description, tbl_schema, tbl_type, tbl_ttl, history_ttl, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL + " limit 1";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s.%s set description = ?, tbl_schema = ?, tbl_ttl = ?, hist_ttl = ?, updated_at = ?" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s.%s where db_name = ?";
	private static final String READ_ALL_COUNT_CQL = "select count(*) from %s.%s where db_name = ?";

	private PreparedStatement existStmt;
	private PreparedStatement readAllCountStmt;

	public TableRepository(Session session, String keyspace)
	{
		super(session, keyspace);
		addObserver(new StateChangeEventingObserver<Table>(new CollectionEventFactory()));
		initialize();
	}

	protected void initialize()
	{
		existStmt = session().prepare(String.format(EXISTENCE_CQL, keyspace(), Tables.BY_ID));
		readAllCountStmt = session().prepare(String.format(READ_ALL_COUNT_CQL, keyspace(), Tables.BY_ID));
	}

	public boolean exists(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty()) return false;

		BoundStatement bs = new BoundStatement(existStmt);
		bindIdentity(bs, identifier);
		return (session().execute(bs).one().getLong(0) > 0);
	}

	public long countAll(String namespace)
	{
		BoundStatement bs = new BoundStatement(readAllCountStmt);
		bs.bind(namespace);
		return (session().execute(bs).one().getLong(0));
	}

	@Override
	protected void bindCreate(BoundStatement bs, Table entity)
	{
		bs.bind(entity.name(),
			entity.database().name(),
			entity.description(),
		    entity.createdAt(),
		    entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Table entity)
	{
		bs.bind(entity.description(),
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
		c.createdAt(row.getDate(Columns.CREATED_AT));
		c.updatedAt(row.getDate(Columns.UPDATED_AT));
		return c;
	}

	private class CollectionEventFactory
	implements EventFactory<Table>
	{
		@Override
		public Object newCreatedEvent(Table object)
		{
			return new TableCreatedEvent(object);
		}

		@Override
		public Object newUpdatedEvent(Table object)
		{
			return new TableUpdatedEvent(object);
		}

		@Override
		public Object newDeletedEvent(Table object)
		{
			return new TableDeletedEvent(object);
		}
	}

	@Override
    public boolean dropSchema()
    {
	    // TODO Auto-generated method stub
	    return false;
    }

	@Override
    public boolean createSchema()
    {
	    // TODO Auto-generated method stub
	    return false;
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
