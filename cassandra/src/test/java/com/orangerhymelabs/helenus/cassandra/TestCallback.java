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

import com.google.common.util.concurrent.FutureCallback;

/**
 * @author tfredrich
 * @since May 11, 2015
 */

public class TestCallback<T>
implements FutureCallback<T>
{
	private T entity;
	private Throwable throwable;

	@Override
    public void onSuccess(T result)
    {
		this.entity = result;
		alert();
    }

	@Override
    public void onFailure(Throwable t)
    {
		this.throwable = t;
		alert();
    }

	private synchronized void alert()
	{
		notifyAll();
	}

	public void clear()
	{
		this.entity = null;
		this.throwable = null;
	}

	public boolean isEmpty()
	{
		return (entity == null && throwable == null);
	}

	public T entity()
	{
		return entity;
	}

	public Throwable throwable()
	{
		return throwable;
	}
}
