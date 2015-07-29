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
package com.orangerhymelabs.helenusdb.exception;

/**
 * @author tfredrich
 * @since June 17, 2015
 */
public class InvalidIdentifierException
extends StorageException
{
    private static final long serialVersionUID = 9115209045795008070L;

	public InvalidIdentifierException()
	{
	}

	/**
	 * @param message
	 */
	public InvalidIdentifierException(String message)
	{
		super(message);
	}

	/**
	 * @param cause
	 */
	public InvalidIdentifierException(Throwable cause)
	{
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InvalidIdentifierException(String message, Throwable cause)
	{
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public InvalidIdentifierException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
