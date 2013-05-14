package edu.indiana.d2i.htrc.preprocessing;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class PreprocessingUtils {
	private static final Logger logger = Logger
			.getLogger(PreprocessingUtils.class);

	public static List<String> loadVolumePages(String volumeDir,
			final List<String> ignoreSuffix) throws IOException {

		File[] pageFiles = new File(volumeDir).listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				// TODO Auto-generated method stub

				boolean filter = false;
				for (String suffix : ignoreSuffix) {
					if (pathname.getName().endsWith(suffix)) {
						filter = true;
						break;
					}
				}

				return !filter;

			}
		});

		List<String> pages = new ArrayList<String>(pageFiles.length);

		for (File page : pageFiles) {
			InputStream in = new FileInputStream(page);

			try {
				String pageContent = IOUtils.toString(in);
				pages.add(pageContent);
			} finally {
				IOUtils.closeQuietly(in);
			}

		}

		return pages;
	}

	/**
	 * Looks for a hyphen followed by whitespace or a line break. Reconstructs
	 * word and checks to see if the result exists in one of the given
	 * dictionaries (e.g. WordNet or the OS's default spellchecker dictionary).
	 * If so, replaces fragments with reconstructed word.
	 * 
	 * @param pages
	 *            List of pages
	 * @param dicts
	 *            List of dictionaries
	 * @return List of cleaned pages
	 */
	public static List<String> removeLBHyphens(List<String> pages,
			List<Dictionary> dicts) {
		String REGEX = "(\\w+)-\\s+(\\w+)";
		Pattern p = Pattern.compile(REGEX);

		List<String> cleanedPages = new ArrayList<String>(pages.size());

		if (dicts == null) {
			// put all pass dictionary
			dicts = Collections
					.singletonList((Dictionary) new AllPassDictionary());
		}

		for (String page : pages) {
			Matcher m = p.matcher(page);

			StringBuffer sb = new StringBuffer();

			while (m.find()) {
				// left hand side of hyphen + right hand side of hyphen
				String leftSide = page.substring(m.start(1), m.end(1));
				String rightSide = page.substring(m.start(2), m.end(2));
				String reconWord = leftSide + rightSide;

				boolean filter = true;

				// check whether the word is in one of the dictionaries
				for (Dictionary dict : dicts) {
					if (dict.contains(reconWord)) {
						filter = false;
						break;
					}
				}

				if (filter) {
					logger.info(String.format(
							"ignore expression: left = [%s], right = [%s]",
							leftSide, rightSide));

					m.appendReplacement(sb, page.substring(m.start(), m.end()));
				} else {
					logger.info(String.format("reconstructed word: %s",
							reconWord));

					m.appendReplacement(sb, reconWord);
				}

			}

			m.appendTail(sb);

			cleanedPages.add(sb.toString());

		}

		return cleanedPages;
	}

	public static void testRemoveLBHyphens() throws IOException {
		String volumeDir = "volumes\\uc2\\raw";
		List<String> ignoreSuffix = new ArrayList<String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			{
				add(".json");
				add(".log");
			}
		};

		List<String> pages = loadVolumePages(volumeDir, ignoreSuffix);
		removeLBHyphens(pages, null);
	}

	public static void main(String[] args) throws IOException {
		testRemoveLBHyphens();
	}
}
