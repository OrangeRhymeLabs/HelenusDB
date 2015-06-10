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
package com.orangerhymelabs.orangedb.cassandra.meta;

/**
 * @author toddf
 * @since Jun 9, 2015
 */
public class KeyValuePair
{
	private String key;
	private String value;

	public KeyValuePair(String key, String value)
    {
	    super();
	    this.key = key;
	    this.value = value;
    }

	public String key()
	{
		return key;
	}

	public String value()
	{
		return value;
	}
}
