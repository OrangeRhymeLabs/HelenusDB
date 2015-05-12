package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.List;

import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;
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

	public void create(Database entity, ResultCallback<Database> callback)
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

	public void read(String name, ResultCallback<Database> callback)
	{
		databases.readAsync(new Identifier(name), callback);
	}

	public void readAll(ResultCallback<List<Database>> callback)
	{
		databases.readAllAsync(callback);
	}

	public void update(Database entity, ResultCallback<Database> callback)
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

	public void delete(String name, ResultCallback<Database> callback)
    {
		databases.deleteAsync(new Identifier(name), callback);
    }
}
