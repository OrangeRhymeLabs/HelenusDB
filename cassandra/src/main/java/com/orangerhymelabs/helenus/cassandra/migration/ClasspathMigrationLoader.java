package com.orangerhymelabs.helenus.cassandra.migration;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClasspathMigrationLoader
{
	private static final Logger LOG = LoggerFactory.getLogger(ClasspathMigrationLoader.class);

	public Collection<Migration> load(MigrationConfiguration config)
	throws IOException
	{
		String scriptLocation = (config.getScriptLocation().startsWith("/") ? config.getScriptLocation().substring(1) : config.getScriptLocation());
		Collection<File> scriptFiles = loadScriptFiles(scriptLocation);
		Collection<Migration> migrations = new ArrayList<Migration>(scriptFiles.size());

		scriptFiles.forEach(new Consumer<File>()
		{
			@Override
			public void accept(File file)
			{
				if (!file.isDirectory() && file.getName().endsWith(".cql"))
				{
					int version = parseVersion(file.getName());

					if (version > 0)
					{
						Migration m = new Migration();
						m.setVersion(version);
						m.setDescription(file.getName());
						try
						{
							m.setScript(readScriptFile(file));
						}
						catch (IOException e)
						{
							LOG.error("Error loading script file: " + file.getName(), e);
						}

						migrations.add(m);
					}
					else
					{
						LOG.warn("Unable to parse version from script filename. Skipped: " + file.getName());
					}
				}
				else
				{
					LOG.info("Skipping file in migrations directory: " + file.getName());
				}
			}

			private String readScriptFile(File file)
			throws IOException
			{
				StringBuilder sb = new StringBuilder();
				Files.readAllLines(file.toPath(), Charset.defaultCharset()).forEach(new Consumer<String>()
				{
					@Override
					public void accept(String t)
					{
						String trimmed = t.trim();

						if (!trimmed.isEmpty())
						{
							sb.append(t);
						}
					}
				});

				return sb.toString();
			}

			private int parseVersion(String name)
			{
				String[] parts = name.split("_");
				String versionString = parts[0];

				try
				{
					return Integer.valueOf(versionString);
				}
				catch (NumberFormatException e)
				{
					LOG.error("Invalid version format in filename: " + name, e);
					return 0;
				}
			}
		});

		return migrations;
	}

	private Collection<File> loadScriptFiles(String scriptLocation)
	throws IOException
	{
		Enumeration<URL> urls = this.getClass().getClassLoader().getResources(scriptLocation);

		if (!urls.hasMoreElements())
		{
			LOG.warn("Unable to resolve location " + scriptLocation);
			return Collections.emptyList();
		}

		Collection<File> files = new ArrayList<File>();

		while (urls.hasMoreElements())
		{
			try
			{
				Files.walk(Paths.get(urls.nextElement().toURI()))
					.forEach(new Consumer<Path>()
					{
						@Override
						public void accept(Path t)
						{
							files.add(t.toFile());
						}
					});
			}
			catch (URISyntaxException e)
			{
				LOG.warn("Error occurred walking script directory", e);
			}
		}

		return files;
	}
}
