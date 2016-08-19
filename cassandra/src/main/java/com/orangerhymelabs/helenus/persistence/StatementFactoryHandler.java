/*
    Copyright 2016, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.persistence;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.exception.StorageException;

/**
 * @author tfredrich
 * @since 18 Aug 2016
 */
public class StatementFactoryHandler
implements InvocationHandler
{
	private Session session;
	private String keyspace;
	private Map<Method, PreparedStatement> statements = new HashMap<>();

	public StatementFactoryHandler(Session session, String keyspace)
	{
		super();
		this.session = session;
		this.keyspace = keyspace;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
	throws Throwable
	{
		PreparedStatement ps = statements.get(method);

		if (ps != null) return ps;

		Query cql = method.getAnnotation(Query.class);

		if (cql == null) throw new StorageException("No @Query annotation for '" + method.getName() + "'");

		
		ps = session.prepareAsync(String.format(cql.value(), keyspace)).get();
		statements.put(method, ps);
		return ps;
	}
}
