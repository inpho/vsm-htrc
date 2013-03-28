/*
#
# Copyright 2007 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 */
package edu.indiana.d2i.htrc.corpus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Utility class
 * 
 * @author Guangchen Ruan
 * 
 */
public class CorpusProcessingUtils {

	/**
	 * class that represents a mapping between a word and its index in the full
	 * word set
	 * 
	 * @author Guangchen
	 * 
	 */
	public static class MappingTableEntry {
		private String word;
		// index of the word in full word set
		private int idx;

		public MappingTableEntry(String word, int idx) {
			this.word = word;
			this.idx = idx;
		}

		public String getWord() {
			return word;
		}

		public int getIdx() {
			return idx;
		}

	}

	/**
	 * Cleans raw volume content
	 * 
	 * @param volume
	 *            volume content represented as a list of pages
	 */
	public static void cleanVolume(ArrayWritable volume) {
		Text[] pages = (Text[]) volume.get();

		for (int i = 0; i < pages.length; i++) {
			// TODO: apply clean logic to each page

			String pageContent = pages[i].toString();

			/**
			 * Logic to clean the pageContent
			 */

			// Logic goes here

			/**
			 * set the cleaned content back, pageContent is page content after
			 * cleaning
			 */
			pages[i].set(pageContent);
		}

		// set cleaned volume content back
		volume.set(pages);
	}

	/**
	 * Extracts interested words (to form full word set)
	 * 
	 * @param volume
	 *            volume content represented as a list of pages
	 * @return interested words appearing in the specified volume
	 */
	public static Set<String> getWordSet(ArrayWritable volume) {
		Set<String> wordSet = new HashSet<String>();

		Text[] pages = (Text[]) volume.get();

		for (int i = 0; i < pages.length; i++) {
			// TODO: apply ogic to extract interested words from each page

			String pageContent = pages[i].toString();

			/**
			 * Logic to extract the interested words
			 */

			// Logic goes here

			/**
			 * set extracted words to 'wordSet', something like
			 * wordSet.addAll(Set<String> extractedWords) where 'extractedWords'
			 * contains interested words extracted from 'pageContent'. Word
			 * should be converted to lowercase
			 */

		}

		return wordSet;
	}

	/**
	 * Transform cleaned volume of form raw text to indices of words in full
	 * word set
	 * 
	 * @param volume
	 *            volume content represented as a list of pages
	 * @param wordSet
	 *            set of interested words (full word set)
	 */
	public static void transformVolume(ArrayWritable volume,
			List<String> wordSet) {

		Text[] pages = (Text[]) volume.get();

		for (int i = 0; i < pages.length; i++) {

			/**
			 * Need change the RegExp when we are also interested in general
			 * symbols other than words
			 */
			String[] tokens = pages[i].toString().toLowerCase().split("\\W+");

			StringBuilder transformedPage = new StringBuilder();

			int idx = -1;

			for (String token : tokens) {
				idx = wordSet.indexOf(token);

				/**
				 * omit tokens that don't appear in word set
				 */
				if (idx != -1) {
					transformedPage.append(idx + " ");
				}

			}

			// remove ' ' at tail
			pages[i].set(transformedPage.toString().trim());
		}

		// set volume
		volume.set(pages);

	}

	/**
	 * 
	 * @param fullWordSetFilePath
	 *            pointing to file which contains full word set, one word per
	 *            line
	 * @param subWordSetFilePath
	 *            pointing to file which contains subset of full word set
	 *            (interested word for a particular analysis), one word per line
	 * @param mappingTableOutPath
	 *            output file path for mapping table file
	 * @throws IOException
	 */
	public static void generateMappingTable(String fullWordSetFilePath,
			String subWordSetFilePath, String mappingTableOutPath)
			throws IOException {

		List<String> wordSet = new ArrayList<String>();
		List<String> subWordSet = new ArrayList<String>();

		BufferedReader reader = null;
		String line = null;

		// load whole wordset
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fullWordSetFilePath)));

			/* each line is a word */
			while ((line = reader.readLine()) != null)
				wordSet.add(line.trim());

		} finally {
			if (reader != null)
				reader.close();
		}

		reader = null;
		line = null;

		// load sub-wordset
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(subWordSetFilePath)));

			/* each line is a word */
			while ((line = reader.readLine()) != null)
				subWordSet.add(line.trim());

		} finally {
			if (reader != null)
				reader.close();
		}

		BufferedWriter writer = null;

		// write mapping table file, each line is of form: <word> <index of the
		// word in full word set>
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(mappingTableOutPath)));

			int idx = -1;

			for (String word : subWordSet) {
				idx = wordSet.indexOf(word);

				writer.write(word + " " + idx + "\n");

			}

		} finally {
			if (writer != null)
				writer.close();

		}
	}

	/**
	 * extracts indices from mapping table
	 * 
	 * @param mappingTable
	 * @return
	 */
	public static List<Integer> extractIdxFromMappingTable(
			List<MappingTableEntry> mappingTable) {
		List<Integer> mappingIndices = new ArrayList<Integer>();

		for (MappingTableEntry entry : mappingTable) {
			mappingIndices.add(entry.getIdx());
		}

		return mappingIndices;
	}

	/**
	 * convert volume content to indices in sub word set
	 * 
	 * @param volume
	 *            volume content represented as indices to full word set
	 * @param mappingIndices
	 */
	public static void fullWordSet2SubWordSet(ArrayWritable volume,
			List<Integer> mappingIndices) {
		Text[] pages = (Text[]) volume.get();

		for (int i = 0; i < pages.length; i++) {
			String[] indices = pages[i].toString().split(" ");

			StringBuilder pg = new StringBuilder();

			for (int j = 0; j < indices.length; j++) {

				int idx = mappingIndices.indexOf(indices[j]);

				/**
				 * omit words not in the sub set
				 */
				if (idx != -1) {
					pg.append(idx + " ");
				}
			}

			// remove ' ' at tail
			pages[i].set(pg.toString().trim());
		}

		// set volume
		volume.set(pages);
	}

	/**
	 * Sort pages in terms of page order
	 * 
	 * @param pages
	 *            a map maintains mapping of <pageSequenceNum, pageContent>
	 * @return
	 */
	public static SortedMap<String, String> sortPages(Map<String, String> pages) {
		SortedMap<String, String> sorted = new TreeMap<String, String>(
				new Comparator<String>() {

					@Override
					public int compare(String o1, String o2) {
						return Integer.valueOf(o1).compareTo(
								Integer.valueOf(o2));
					}

				});

		sorted.putAll(pages);
		return sorted;
	}
}
