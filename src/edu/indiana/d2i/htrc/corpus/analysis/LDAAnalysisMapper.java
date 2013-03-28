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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.htrc.Constants;
import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils;
import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils.MappingTableEntry;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;
import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalyzer.LDAState;

public class LDAAnalysisMapper extends
		Mapper<Text, TextArrayWritable, Text, LDAState> {

	enum HTRC_CORPUS {
		NUM_LDA_ANALYZED_VOLUMES
	}

	private static Text LDASTATE_KEY = new Text("LDA_STATE_KEY");

	private List<MappingTableEntry> mappingTable = new ArrayList<MappingTableEntry>();
	private List<Integer> mappingIndices = null;

	private List<String> topics = new ArrayList<String>();

	private LDAAnalyzer ldaAnalyzer;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String mappingTableFileName = conf
				.get("user.args.mapping.table.filename");
		String topicsFileName = conf.get("user.args.topics.filename");

		BufferedReader reader = null;
		String line = null;

		// load mapping table
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(mappingTableFileName)));

			/*
			 * each line is a mapping: <word> <index of the word in full word
			 * set>
			 */
			while ((line = reader.readLine()) != null) {
				String trimmedLine = line.trim();
				int idx = trimmedLine.lastIndexOf(' ');
				mappingTable
						.add(new MappingTableEntry(trimmedLine
								.substring(0, idx), Integer
								.parseInt(trimmedLine.substring(idx + 1))));

			}

		} finally {
			if (reader != null)
				reader.close();
		}

		mappingIndices = CorpusProcessingUtils
				.extractIdxFromMappingTable(mappingTable);

		reader = null;
		line = null;

		// load topics
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(topicsFileName)));

			/* Each line is a topic */
			while ((line = reader.readLine()) != null) {
				topics.add(line.trim());
			}

		} finally {
			if (reader != null)
				reader.close();
		}

		// load LDA state, stateFilePath is the path in HDFS
		String stateFilePath = conf.get("user.args.lda.state.filepath");
		int stepSize = conf.getInt("user.args.topdoctable.capacity.stepsize",
				Integer.parseInt(Constants.LDA_ANALYSIS_DEFAULT_STEP_SIZE));

		if (stateFilePath == null) {
			// No previous state for initialization (first iteration)
			ldaAnalyzer = new LDAAnalyzer(mappingTable, topics, stepSize);
		} else {
			// second and following iterations
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader seqFileReader = null;

			try {
				seqFileReader = new SequenceFile.Reader(fs, new Path(
						stateFilePath), conf);

				Text key = (Text) ReflectionUtils.newInstance(
						seqFileReader.getKeyClass(), conf);
				LDAState ldaState = (LDAState) ReflectionUtils.newInstance(
						seqFileReader.getValueClass(), conf);

				// the sequence file should only have one record
				seqFileReader.next(key, ldaState);

				ldaAnalyzer = new LDAAnalyzer(ldaState.getWordsTopicsTable(),
						ldaState.getTopicsDocumentsTable(), mappingTable,
						topics);
			} finally {
				IOUtils.closeStream(seqFileReader);
			}

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		// write partial LDA state
		context.write(LDASTATE_KEY, ldaAnalyzer.getLDAState());
	}

	@Override
	public void map(Text key, TextArrayWritable value, Context context)
			throws IOException, InterruptedException {
		// key is volume id, value is volume content (list of pages), key and
		// value are extracted from sequence file

		CorpusProcessingUtils.fullWordSet2SubWordSet(value, mappingIndices);

		// update WordsTopicsTable
		ldaAnalyzer.updateWordsTopicsTable(value);

		// update TopcisDocumentsTable
		ldaAnalyzer.updateTopcisDocumentsTable(key.toString(), value);

		// report progress
		context.getCounter(HTRC_CORPUS.NUM_LDA_ANALYZED_VOLUMES).increment(1);
	}
}
