package com.orangerhymelabs.orangedb.cassandra.table;

import java.util.List;

import com.orangerhymelabs.orangedb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;
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

	public void create(Table entity, ResultCallback<Table> callback)
	{
		databases.existsAsync(entity.database().getId(), new ResultCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (result)
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

	public void read(String database, String table, ResultCallback<Table> callback)
	{
		tables.readAsync(new Identifier(database, table), callback);
	}

	public void readAll(String database, ResultCallback<List<Table>> callback)
	{
		databases.existsAsync(new Identifier(database), new ResultCallback<Boolean>()
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

	public void update(Table entity, ResultCallback<Table> callback)
	{
		ValidationEngine.validateAndThrow(entity);
		tables.updateAsync(entity, callback);
	}

	public void delete(Identifier id, ResultCallback<Table> callback)
	{
		tables.deleteAsync(id, callback);
	}
}
