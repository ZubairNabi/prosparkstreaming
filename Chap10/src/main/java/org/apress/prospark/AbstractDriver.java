package org.apress.prospark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FilenameUtils;
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
					String ext = FilenameUtils.getExtension(f.getPath());
					if (ext.equals("zip")) {
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
					} else if (ext.equals("gz")) {
						LOG.info(String.format("Feeding file %s", f.getName()));
						try (BufferedReader br = new BufferedReader(
								new InputStreamReader(new GZIPInputStream(new FileInputStream(f))))) {
							// skip header
							br.readLine();
							String line;
							while ((line = br.readLine()) != null) {
								sendRecord(line);
							}
						}
					} else if (ext.equals("dat") || ext.equals("json")) {
						LOG.info(String.format("Feeding dat file %s", f.getName()));
						try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)))) {
							String line;
							while ((line = br.readLine()) != null) {
								sendRecord(line);
							}
						}
					} else {
						LOG.warn("Unsupported file type: " + f.getName());
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