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
package com.orangerhymelabs.helenus.cassandra;

import java.util.List;

import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 * @param <T> The type stored in this repository.
 */
public interface Repository<T>
{
	public T create(T entity);
	public boolean exists(Identifier id);
	public T update(T entity);
	public boolean delete(Identifier id);
	public T read(Identifier id);
	public List<T> readAll(Object... parms);
	public List<T> readIn(Identifier... ids);
}
