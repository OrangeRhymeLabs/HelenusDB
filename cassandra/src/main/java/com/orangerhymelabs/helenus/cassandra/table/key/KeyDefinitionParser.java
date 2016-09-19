/**
 * 
 */
package com.orangerhymelabs.helenus.cassandra.table.key;

import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.table.key.ClusteringKeyComponent.Ordering;

/**
 * Used to parse the View.keys property into something that can pull values from a BSONObject.
 * 
 * @author tfredrich
 * @since 1 Sept 2016
 */
public class KeyDefinitionParser
{
	private enum ParseState
	{
		PARTITION_KEY,
		CLUSTER_KEY;

		public boolean isPartitionKey()
		{
			return PARTITION_KEY.equals(this);
		}
	}

	/**
	 * Used to parse the View.keys property into something that can retrieve values from a BSONObject.
	 * 
	 * Key string is of the form:
	 * a:uuid 									// partition key only
	 * ((a:uuid, b:text), -c:timestamp, +d:int)	// partition key + clustering key, with (or without) sort order
	 * 
	 * @param keys a string defining the key structure of a View. Cannot be null or empty.
	 * @return a new KeyDefinition instance.
	 * @throws KeyDefinitionException if the string is invalid.
	 */
	public KeyDefinition parse(String keys)
	throws KeyDefinitionException
	{
		if (keys == null || keys.isEmpty()) throw new KeyDefinitionException("Key string null or empty");

		KeyDefinition definition = new KeyDefinition();
		char[] chars = keys.toCharArray();
		ParseState state = ParseState.PARTITION_KEY;
		StringBuilder phrase = new StringBuilder();
		int i = 0;
		int depth = 0;

		do
		{
			switch(chars[i])
			{
				case '(':
					if (state.isPartitionKey()) ++depth;
					else throw new KeyDefinitionException("Misplaced '('");
					break;
				case ')':
					processPhrase(phrase, definition, state);
					--depth;
					if (depth < 0) throw new KeyDefinitionException("Misplaced ')'");
					if (state.isPartitionKey())
					{
						state = ParseState.CLUSTER_KEY;
					}
					break;
				case ',':
					processPhrase(phrase, definition, state);
					break;
				default:
					if (Character.isWhitespace(chars[i])) break;

					phrase.append(chars[i]);
					break;
			}
		}
		while (++i < chars.length);

		if (depth != 0) throw new KeyDefinitionException("Unbalanced parenthises: " + keys);

		processPhrase(phrase, definition, state);
		return definition;
	}

	private void processPhrase(StringBuilder phrase, KeyDefinition definition, ParseState state)
	throws KeyDefinitionException
	{
		if (phrase.length() == 0) return;

		if (state.isPartitionKey()) definition.addPartitionKey(processPartitionPhrase(phrase.toString()));
		else definition.addClusteringKey(processClusteringPhrase(phrase.toString()));
		phrase.setLength(0);
	}

	private KeyComponent processPartitionPhrase(String phrase)
	throws KeyDefinitionException
	{
		String[] p = phrase.split(":");

		if (p.length != 2) throw new KeyDefinitionException("Invalid partition key phrase: " + phrase);
		else if (!Character.isAlphabetic(p[0].charAt(0)))
		{
			throw new KeyDefinitionException("Invalid partitioning key property name: " + phrase);
		}
		try
		{
			return new KeyComponent(p[0], DataTypes.from(p[1]));
		}
		catch (IllegalStateException e)
		{
			throw new KeyDefinitionException(e.getMessage());
		}
	}

	private ClusteringKeyComponent processClusteringPhrase(String phrase)
	throws KeyDefinitionException
	{
		String[] p = phrase.split(":");

		if (p.length != 2) throw new KeyDefinitionException("Invalid clustering key phrase: " + phrase);

		Ordering order = Ordering.ASC;
		String property = p[0];

		if (property.startsWith("-"))
		{
			property = property.substring(1);
			order = Ordering.DESC;
		}
		else if (property.startsWith("+"))
		{
			property = property.substring(1);
		}
		else if (!Character.isAlphabetic(property.charAt(0)))
		{
			throw new KeyDefinitionException("Invalid clustering key property name: " + phrase);
		}

		try
		{
			return new ClusteringKeyComponent(property, DataTypes.from(p[1]), order);
		}
		catch (IllegalStateException e)
		{
			throw new KeyDefinitionException(e.getMessage());
		}
	}
}
