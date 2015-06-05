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
package com.orangerhymelabs.orangedb.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.FutureCallback;

/**
 * @author toddf
 * @since May 11, 2015
 */

public class ReadInCallback<T>
implements FutureCallback<T>
{
	private List<T> entities = new ArrayList<T>();
	private Throwable throwable;
	private int threshold = 0;
	private int count = 0;

	public ReadInCallback()
	{
		super();
	}

	public ReadInCallback(int threshold)
	{
		this();
		this.threshold = threshold;
	}

	@Override
    public void onSuccess(T result)
    {
		this.entities.add(result);
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
		if (++count >= threshold)
		{
			notifyAll();
		}
	}

	public void clear()
	{
		this.entities.clear();
		this.throwable = null;
	}

	public boolean isEmpty()
	{
		return (entities.isEmpty() && throwable == null);
	}

	public List<T> entities()
	{
		return entities;
	}

	public Throwable throwable()
	{
		return throwable;
	}
}
