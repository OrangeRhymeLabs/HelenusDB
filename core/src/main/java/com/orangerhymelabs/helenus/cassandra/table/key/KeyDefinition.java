package com.orangerhymelabs.helenus.cassandra.table.key;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.bson.BSONObject;

import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since 1 Sept 2016
 */
public class KeyDefinition
{
	private List<KeyComponent> partitionKey;
	private List<ClusteringKeyComponent> clusteringKey;

	public KeyDefinition addPartitionKey(KeyComponent component)
	{
		if (partitionKey == null)
		{
			partitionKey = new ArrayList<>();
		}

		partitionKey.add(component);
		return this;
	}

	public KeyDefinition addClusteringKey(ClusteringKeyComponent component)
	{
		if (clusteringKey == null)
		{
			clusteringKey = new ArrayList<>();
		}

		clusteringKey.add(component);
		return this;
	}

	public int size()
	{
		return ((hasPartitionKey() ? partitionKey.size() : 0) + (hasClusteringKey() ? clusteringKey.size() : 0));
	}

	public boolean hasPartitionKey()
	{
		return (partitionKey != null && !partitionKey.isEmpty());
	}

	public boolean hasClusteringKey()
	{
		return (clusteringKey != null && !clusteringKey.isEmpty());
	}

	/**
	 * Pulls the properties that correspond to this KeyDefinition into an Identifier instance.
	 * If one or more of the properties are missing, returns null.
	 * 
	 * @param bson a BSONObject.
	 * @return an Identifier instance or null.
	 * @throws KeyDefinitionException if the bson document is missing any properties in the key definition identifier. 
	 */
	public Identifier identifier(BSONObject bson)
	throws KeyDefinitionException
	{
		Identifier identifier = new Identifier();
		List<String> missingProperties = new ArrayList<>(0);

		if (hasPartitionKey())
		{
			appendToIdentifierFromBson(partitionKey, bson, identifier, missingProperties);
		}

		if (hasClusteringKey())
		{
			appendToIdentifierFromBson(clusteringKey, bson, identifier, missingProperties);
		}
		
		if (identifier.size() != size())
		{
			throw new KeyDefinitionException("Missing properties: " + String.join(", ", missingProperties));
		}

		return identifier;
	}

	public boolean isValid()
	{
		if (hasPartitionKey()) return true;
		return false;
	}

	public String asColumns()
	{
		StringBuilder sb = new StringBuilder();

		if (hasPartitionKey())
		{
			appendAsColumns(partitionKey, sb);
		}

		if (hasClusteringKey())
		{
			sb.append(",");
			appendAsColumns(clusteringKey, sb);
		}

		return sb.toString();
	}

	public String asPrimaryKey()
	{
		StringBuilder sb = new StringBuilder("primary key (");

		if (hasClusteringKey())
		{
			sb.append("(");
		}

		appendAsProperties(partitionKey, sb, ",");

		if (hasClusteringKey())
		{
			sb.append("),");
			appendAsProperties(clusteringKey, sb, ",");
		}

		sb.append(")");
		return sb.toString();
	}

	public String asClusteringKey()
	{
		StringBuilder sb = new StringBuilder();

		if (hasDescendingSort(clusteringKey))
		{
			appendClusteringOrderPhrase(clusteringKey, sb);
		}

		return sb.toString();
	}

	public String asSelectProperties()
	{
		StringBuilder sb = new StringBuilder();
		appendAsProperties(partitionKey, sb, ",");
		appendAsProperties(clusteringKey, sb, ",");
		return sb.toString();
	}

	public String asQuestionMarks(int extras)
	{
		String[] qms = new String[size() + extras];
		Arrays.fill(qms, "?");
		return String.join(",", qms);
	}

	public String asIdentityClause()
	{
		StringBuilder sb = new StringBuilder();
		appendAsAssignments(partitionKey, sb, " and ");

		if (hasClusteringKey())
		{
			sb.append(" and ");
			appendAsAssignments(clusteringKey, sb, " and ");
		}

		return sb.toString();
	}

	public Object asPartitionIdentityClause()
	{
		StringBuilder sb = new StringBuilder();
		appendAsAssignments(partitionKey, sb, " and ");
		return sb.toString();
	}

	private void appendToIdentifierFromBson(List<? extends KeyComponent> components, BSONObject bson, Identifier identifier, List<String> missingProperties)
	{
		components.forEach(new Consumer<KeyComponent>()
		{
			@Override
			public void accept(KeyComponent t)
			{
				Object o = bson.get(t.property());

				if (o != null)
				{
					identifier.add(o);
				}
				else
				{
					missingProperties.add(t.property());
				}
			}
		});
	}

	private void appendAsColumns(List<? extends KeyComponent> components, StringBuilder builder)
	{
		if (components == null || components.isEmpty()) return;

		Iterator<? extends KeyComponent> iterator = components.iterator();
		KeyComponent component = iterator.next();
		builder
			.append(component.property())
			.append(" ")
			.append(component.type().cassandraType());

		while(iterator.hasNext())
		{
			component = iterator.next();
			builder
				.append(",")
				.append(component.property())
				.append(" ")
				.append(component.type().cassandraType());
		}
	}

	private void appendAsProperties(List<? extends KeyComponent> components, StringBuilder builder, String delimiter)
	{
		if (components == null || components.isEmpty()) return;

		Iterator<? extends KeyComponent> iterator = components.iterator();
		builder.append(iterator.next().property());

		while(iterator.hasNext())
		{
			builder
				.append(delimiter)
				.append(iterator.next().property());
		}
	}

	private void appendAsAssignments(List<? extends KeyComponent> components, StringBuilder builder, String delimiter)
	{
		if (components == null || components.isEmpty()) return;

		Iterator<? extends KeyComponent> iterator = components.iterator();
		KeyComponent component = iterator.next();
		builder
			.append(component.property())
			.append(" = ?");

		while(iterator.hasNext())
		{
			component = iterator.next();
			builder
				.append(" and ")
				.append(component.property())
				.append(" = ?");
		}
	}

	private boolean hasDescendingSort(List<ClusteringKeyComponent> components)
	{
		if (components == null) return false;

		return components.stream().anyMatch(new Predicate<ClusteringKeyComponent>()
		{
			@Override
			public boolean test(ClusteringKeyComponent t)
			{
				return ((t.order().isDescending()));
			}
		});
	}

	private void appendClusteringOrderPhrase(List<ClusteringKeyComponent> components, StringBuilder builder)
	{
		builder.append("with clustering order by (");
		components.forEach(new Consumer<ClusteringKeyComponent>()
		{
			private boolean isFirst = true;

			@Override
			public void accept(ClusteringKeyComponent t)
			{
				if (!isFirst)
				{
					builder.append(",");
				}

				builder.append(t.property())
					.append(" ")
					.append(t.order());
				isFirst = false;
			}
		});
		builder.append(")");
	}

	public List<KeyComponent> components()
	{
		ArrayList<KeyComponent> c = new ArrayList<>(size());
		c.addAll(partitionKey);

		if (clusteringKey != null)
		{
			c.addAll(clusteringKey);
		}

		return c;
	}
}
