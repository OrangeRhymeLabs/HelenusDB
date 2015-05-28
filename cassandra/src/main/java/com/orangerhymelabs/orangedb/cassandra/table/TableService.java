package com.orangerhymelabs.orangedb.cassandra.table;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

public class TableService
{
	private TableRepository tables;
	private DatabaseRepository databases;

	public TableService(DatabaseRepository databaseRepository, TableRepository tableRepository)
	{
		super();
		this.databases = databaseRepository;
		this.tables = tableRepository;
	}

	public Table create(Table entity)
	{
		if (!databases.exists(entity.database().getId()))
		{
			throw new ItemNotFoundException("Database not found: " + entity.database());
		}

		ValidationEngine.validateAndThrow(entity);
		return tables.create(entity);
	}

	public void createAsync(Table entity, FutureCallback<Table> callback)
	{
		databases.existsAsync(entity.database().getId(), new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (!result)
					{
						callback.onFailure(new ItemNotFoundException("Database not found: " + entity.database()));
					}
					else
					{
						try
						{
							ValidationEngine.validateAndThrow(entity);
							tables.createAsync(entity, callback);
						}
						catch(ValidationException e)
						{
							callback.onFailure(e);
						}
					}
                }

				@Override
                public void onFailure(Throwable t)
                {
					callback.onFailure(t);
                }
			}
		);
	}

	public Table read(String database, String table)
	{
		return tables.read(new Identifier(database, table));
	}

	public void readAsync(String database, String table, FutureCallback<Table> callback)
	{
		tables.readAsync(new Identifier(database, table), callback);
	}

	public List<Table> readAll(String database)
	{
		Identifier id = new Identifier(database);
		if (!databases.exists(id))
		{
			throw new ItemNotFoundException("Database not found: " + database);
		}

		return tables.readAll(id);
	}

	public void readAllAsync(String database, FutureCallback<List<Table>> callback)
	{
		databases.existsAsync(new Identifier(database), new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (!result)
					{
						callback.onFailure(new ItemNotFoundException("Database not found: " + database));
					}
					else
					{
						tables.readAllAsync(callback, database);
					}
                }

				@Override
                public void onFailure(Throwable t)
                {
					callback.onFailure(t);
                }
			}
		);
	}

	public Table update(Table entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return tables.update(entity);
	}

	public void updateAsync(Table entity, FutureCallback<Table> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(entity);
			tables.updateAsync(entity, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public void delete(String database, String table)
	{
		tables.delete(new Identifier(database, table));
	}

	public void deleteAsync(String database, String table, FutureCallback<Table> callback)
	{
		tables.deleteAsync(new Identifier(database, table), callback);
	}
}
