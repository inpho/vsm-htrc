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
package edu.indiana.d2i.htrc.corpus.analysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils.MappingTableEntry;
import edu.indiana.d2i.htrc.exception.DimensionMismatchException;

public class LDAAnalyzer {
	private List<MappingTableEntry> mappingTable;
	private List<String> topics;
	private WordsTopicsTable wordsTopicsTable;
	private TopicsDocumentsTable topicsDocumentsTable;

	/**
	 * constructor used for the first iteration. See {@link MappingTableEntry}
	 * 
	 * @param mappingTable
	 * @param topics
	 */
	public LDAAnalyzer(List<MappingTableEntry> mappingTable,
			List<String> topics, int stepSize) {
		this.mappingTable = mappingTable;
		this.topics = topics;

		wordsTopicsTable = new WordsTopicsTable(mappingTable.size(),
				topics.size());
		topicsDocumentsTable = new TopicsDocumentsTable(topics.size(), stepSize);
	}

	/**
	 * constructor that uses tables derived from previous iteration to
	 * initialize tables of current iteration
	 * 
	 * @param wordsTopicsTable
	 * @param topicsDocumentsTable
	 * @param mappingTable
	 * @param topics
	 */
	public LDAAnalyzer(WordsTopicsTable wordsTopicsTable,
			TopicsDocumentsTable topicsDocumentsTable,
			List<MappingTableEntry> mappingTable, List<String> topics) {
		this.wordsTopicsTable = wordsTopicsTable;
		this.topicsDocumentsTable = topicsDocumentsTable;
		this.mappingTable = mappingTable;
		this.topics = topics;
	}

	/**
	 * 
	 * @param ldaState
	 * @return
	 */
	public static boolean isConverged(LDAState ldaState) {
		// TODO check whether LDA state is converged

		return true;
	}

	public void updateWordsTopicsTable(ArrayWritable volume) {
		// TODO update the WordsTopicsTable by given volume
	}

	public void updateTopcisDocumentsTable(String documentID,
			ArrayWritable volume) {
		// TODO update the TopcisDocumentsTable by given volume
	}

	public LDAState getLDAState() {
		return new LDAState(wordsTopicsTable, topicsDocumentsTable);
	}

	public static class LDAState implements Writable {
		private WordsTopicsTable wordsTopicsTable;

		private TopicsDocumentsTable topicsDocumentsTable;

		public LDAState() {

		}

		public LDAState(WordsTopicsTable wordsTopicsTable,
				TopicsDocumentsTable topicsDocumentsTable) {
			this.wordsTopicsTable = wordsTopicsTable;
			this.topicsDocumentsTable = topicsDocumentsTable;
		}

		public WordsTopicsTable getWordsTopicsTable() {
			return wordsTopicsTable;
		}

		public void setWordsTopicsTable(WordsTopicsTable wordsTopicsTable) {
			this.wordsTopicsTable = wordsTopicsTable;
		}

		public TopicsDocumentsTable getTopicsDocumentsTable() {
			return topicsDocumentsTable;
		}

		public void setTopicsDocumentsTable(
				TopicsDocumentsTable topicsDocumentsTable) {
			this.topicsDocumentsTable = topicsDocumentsTable;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

			wordsTopicsTable = new WordsTopicsTable();
			topicsDocumentsTable = new TopicsDocumentsTable();

			wordsTopicsTable.readFields(in);
			topicsDocumentsTable.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub

			wordsTopicsTable.write(out);
			topicsDocumentsTable.write(out);
		}
	}

	/**
	 * Class that represents words-topics table
	 * 
	 * @author Guangchen
	 * 
	 */
	public static class WordsTopicsTable implements Writable {
		private float[][] wordsTopicsTable = null;

		public WordsTopicsTable() {
		}

		public WordsTopicsTable(int numWords, int numTopics) {

			wordsTopicsTable = new float[numWords][numTopics];

		}

		public float[][] getWordsTopicsTable() {
			return wordsTopicsTable;
		}

		public int getNumWords() {
			return wordsTopicsTable.length;
		}

		public int getNumTopics() {
			return wordsTopicsTable[0].length;
		}

		/**
		 * initialize with specified value
		 * 
		 * @param value
		 */
		public void init(float value) {
			for (int i = 0; i < wordsTopicsTable.length; i++)
				for (int j = 0; j < wordsTopicsTable[0].length; j++)
					wordsTopicsTable[i][j] = value;
		}

		public void setValue(int wordIndex, int topicIndex, float value) {

			if (!(wordIndex >= 0 && wordIndex < wordsTopicsTable.length)
					|| !(topicIndex >= 0 && topicIndex < wordsTopicsTable[0].length)) {
				throw new IndexOutOfBoundsException(
						String.format(
								"Index [%d, %d] out of boundary, the dimension of the WordsTopics table is [%d, %d]",
								wordIndex, topicIndex, wordsTopicsTable.length,
								wordsTopicsTable[0].length));
			}

			wordsTopicsTable[wordIndex][topicIndex] = value;
		}

		public float getValue(int wordIndex, int topicIndex) {

			if (!(wordIndex >= 0 && wordIndex < wordsTopicsTable.length)
					|| !(topicIndex >= 0 && topicIndex < wordsTopicsTable[0].length)) {
				throw new IndexOutOfBoundsException(
						String.format(
								"Index [%d, %d] out of boundary, the dimension of the WordsTopics table is [%d, %d]",
								wordIndex, topicIndex, wordsTopicsTable.length,
								wordsTopicsTable[0].length));
			}

			return wordsTopicsTable[wordIndex][topicIndex];
		}

		public void mergeTables(WordsTopicsTable table) {

			if (wordsTopicsTable == null) {
				wordsTopicsTable = table.getWordsTopicsTable();

				return;
			}

			/**
			 * make sure dimensions are the same
			 */

			if ((wordsTopicsTable.length != table.getNumWords())
					|| (wordsTopicsTable[0].length != table.getNumTopics())) {
				throw new DimensionMismatchException(
						String.format(
								"table 1 with dims = [%d, %d] doesn't match table 2 with dims = [%d, %d]",
								wordsTopicsTable.length,
								wordsTopicsTable[0].length,
								table.getNumWords(), table.getNumTopics()));
			}

			// merge
			for (int i = 0; i < wordsTopicsTable.length; i++)
				for (int j = 0; j < wordsTopicsTable[0].length; j++)
					wordsTopicsTable[i][j] += table.getValue(i, j);

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

			int numWords = in.readInt();
			int numTopics = in.readInt();

			wordsTopicsTable = new float[numWords][numTopics];

			for (int i = 0; i < numWords; i++)
				for (int j = 0; j < numTopics; j++)
					wordsTopicsTable[i][j] = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub

			// write number of words
			out.writeInt(wordsTopicsTable.length);

			// write number of topics
			out.writeInt(wordsTopicsTable[0].length);

			// write table
			for (int i = 0; i < wordsTopicsTable.length; i++)
				for (int j = 0; j < wordsTopicsTable[0].length; j++)
					out.writeFloat(wordsTopicsTable[i][j]);
		}
	}

	public static class TopicsDocumentsTable implements Writable {

		/**
		 * Since each map task deals with a subset of documents and we don't
		 * know the number of documents to be processed by each map task in
		 * advance, we need to dynamically adjust the table size (number of
		 * columns), below variables 'topicsDocumentsTable[0].length' is used to
		 * indicate current capacity of the table and 'documentsID.size()' is
		 * used to indicate number of documents stored in this table, we always
		 * have documentsID.size() <= topicsDocumentsTable[0].length. We
		 * dynamically increase the table capacity by 'stepSize' at a time when
		 * the table doesn't have room for new documents
		 */
		private float[][] topicsDocumentsTable = null;

		private int stepSize;
		private List<String> documentsID = new ArrayList<String>();

		public TopicsDocumentsTable() {

		}

		public TopicsDocumentsTable(int numTopics, int stepSize) {

			topicsDocumentsTable = new float[numTopics][stepSize];

			this.stepSize = stepSize;
		}

		/**
		 * compact the table
		 */
		private void compact() {
			float[][] newTable = new float[topicsDocumentsTable.length][documentsID
					.size()];

			// copy existing values
			for (int i = 0; i < topicsDocumentsTable.length; i++)
				for (int j = 0; j < documentsID.size(); j++) {
					newTable[i][j] = topicsDocumentsTable[i][j];
				}

			topicsDocumentsTable = newTable;
		}

		/**
		 * increase table capacity
		 */
		private void increaseCapacity() {

			float[][] newTable = new float[topicsDocumentsTable.length][topicsDocumentsTable[0].length
					+ stepSize];

			// copy existing values
			for (int i = 0; i < topicsDocumentsTable.length; i++)
				for (int j = 0; j < topicsDocumentsTable[0].length; j++) {
					newTable[i][j] = topicsDocumentsTable[i][j];
				}

			topicsDocumentsTable = newTable;
		}

		/**
		 * 
		 * @param values
		 *            list of topic values
		 * @param documentID
		 */
		public void setDocument(float[] values, String documentID) {
			if (topicsDocumentsTable.length != values.length) {
				throw new DimensionMismatchException(
						String.format(
								"Length mismatch, # of topics = [%d], # of passed in topic values = [%d]",
								topicsDocumentsTable.length, values.length));
			}

			if (documentsID.size() >= topicsDocumentsTable[0].length) {
				increaseCapacity();
			}

			for (int i = 0; i < topicsDocumentsTable.length; i++) {
				topicsDocumentsTable[i][documentsID.size()] = values[i];
			}

			documentsID.add(documentID);
		}

		public float[][] getTopicsDocumentsTable() {
			return topicsDocumentsTable;
		}

		public List<String> getDocumentsID() {
			return documentsID;
		}

		public int getStepSize() {
			return stepSize;
		}

		public int getNumTopics() {
			return topicsDocumentsTable.length;
		}

		public int getNumDocuments() {
			return documentsID.size();
		}

		public void concatenateTables(TopicsDocumentsTable table) {

			if (topicsDocumentsTable == null) {
				topicsDocumentsTable = table.getTopicsDocumentsTable();
				documentsID = table.getDocumentsID();
				stepSize = table.getStepSize();

				return;
			}

			/**
			 * Make sure the number of topics should be the same
			 */
			if (topicsDocumentsTable.length != table.getNumTopics())
				throw new DimensionMismatchException(
						String.format(
								"table 1 with [%d] topics doesnt match table 2 with [%d] topics",
								topicsDocumentsTable.length,
								table.getNumTopics()));

			// concatenate
			float[][] newTable = new float[topicsDocumentsTable.length][topicsDocumentsTable[0].length
					+ table.getNumDocuments()];

			for (int i = 0; i < topicsDocumentsTable.length; i++)
				for (int j = 0; j < topicsDocumentsTable[0].length; j++)
					newTable[i][j] = topicsDocumentsTable[i][j];

			for (int i = 0; i < topicsDocumentsTable.length; i++)
				for (int j = topicsDocumentsTable[0].length; j < topicsDocumentsTable[0].length
						+ table.getNumDocuments(); j++)
					newTable[i][j] = table.getTopicsDocumentsTable()[i][j
							- topicsDocumentsTable[0].length];

			topicsDocumentsTable = newTable;

			// add documentsID
			documentsID.addAll(table.getDocumentsID());
		}

		/**
		 * sort table by documentID
		 */
		public void sortTable() {
			List<String> unsortedDocumentsID = new ArrayList<String>();
			unsortedDocumentsID.addAll(documentsID);

			Collections.sort(documentsID);

			float[][] newTable = new float[topicsDocumentsTable.length][topicsDocumentsTable[0].length];

			for (int j = 0; j < documentsID.size(); j++) {
				int idx = unsortedDocumentsID.indexOf(documentsID.get(j));

				for (int i = 0; i < newTable.length; i++) {

					newTable[i][j] = topicsDocumentsTable[i][idx];
				}
			}

			topicsDocumentsTable = newTable;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

			int numTopics = in.readInt();
			int numDocuments = in.readInt();

			topicsDocumentsTable = new float[numTopics][numDocuments];

			for (int i = 0; i < numTopics; i++)
				for (int j = 0; j < numDocuments; j++)
					topicsDocumentsTable[i][j] = in.readFloat();

			documentsID = new ArrayList<String>();

			// read using ArrayWritable
			ArrayWritable aw = new ArrayWritable(Text.class);
			aw.readFields(in);

			Text[] ids = (Text[]) aw.get();

			for (int i = 0; i < ids.length; i++)
				documentsID.add(ids[i].toString());

			stepSize = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub

			// compact table first
			compact();

			// write number of topics
			out.writeInt(topicsDocumentsTable.length);

			// write number of documents
			out.writeInt(topicsDocumentsTable[0].length);

			// write table
			for (int i = 0; i < topicsDocumentsTable.length; i++)
				for (int j = 0; j < topicsDocumentsTable[0].length; j++)
					out.writeFloat(topicsDocumentsTable[i][j]);

			// write documentsID, using ArrayWritable as helper class
			ArrayWritable aw = new ArrayWritable(Text.class);

			Text[] ids = new Text[documentsID.size()];
			for (int i = 0; i < documentsID.size(); i++) {
				ids[i] = new Text(documentsID.get(i));
			}

			aw.set(ids);
			aw.write(out);

			// write stepSize
			out.writeInt(stepSize);
		}

	}
}
