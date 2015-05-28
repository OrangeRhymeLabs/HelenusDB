package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

public class DatabaseService
{
	private DatabaseRepository databases;
	
	public DatabaseService(DatabaseRepository databaseRepository)
	{
		super();
		this.databases = databaseRepository;
	}

	public void create(Database entity, FutureCallback<Database> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(entity);
			databases.createAsync(entity, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public void read(String name, FutureCallback<Database> callback)
	{
		databases.readAsync(new Identifier(name), callback);
	}

	public void readAll(FutureCallback<List<Database>> callback)
	{
		databases.readAllAsync(callback);
	}

	public void update(Database entity, FutureCallback<Database> callback)
    {
		try
		{
			ValidationEngine.validateAndThrow(entity);
			databases.updateAsync(entity, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
    }

	public void delete(String name, FutureCallback<Database> callback)
    {
		databases.deleteAsync(new Identifier(name), callback);
    }
}
