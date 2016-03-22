package org.apress.prospark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class AbstractDriver {

	private static final Logger LOG = LogManager.getLogger(AbstractDriver.class);

	private String path;

	public AbstractDriver(String path) {
		this.path = path;
	}

	public abstract void init() throws Exception;

	public abstract void close() throws Exception;

	public abstract void sendRecord(String record) throws Exception;

	public void execute() throws Exception {

		try {
			init();
			File dirPath = new File(path);
			if (dirPath.isDirectory()) {
				File[] files = new File(path).listFiles();
				for (File f : files) {
					LOG.info(String.format("Feeding zipped file %s", f.getName()));
					ZipFile zFile = null;
					try {
						zFile = new ZipFile(f);
						Enumeration<? extends ZipEntry> zEntries = zFile.entries();

						while (zEntries.hasMoreElements()) {
							ZipEntry zEntry = zEntries.nextElement();
							LOG.info(String.format("Feeding file %s", zEntry.getName()));
							try (BufferedReader br = new BufferedReader(
									new InputStreamReader(zFile.getInputStream(zEntry)))) {
								// skip header
								br.readLine();
								String line;
								while ((line = br.readLine()) != null) {
									sendRecord(line);
								}
							}
						}
					} catch (IOException e) {
						LOG.error(e.getMessage());
					} finally {
						if (zFile != null) {
							try {
								zFile.close();
							} catch (IOException e) {
								LOG.error(e.getMessage());
							}
						}
					}
				}
			} else {
				LOG.error(String.format("Path %s is not a directory", path));
			}
		} finally {
			close();
		}
	}
}