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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalyzer.LDAState;
import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalyzer.TopicsDocumentsTable;
import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalyzer.WordsTopicsTable;

public class LDAAnalysisReducer extends Reducer<Text, LDAState, Text, LDAState> {

	@Override
	public void reduce(Text key, Iterable<LDAState> values, Context context)
			throws IOException, InterruptedException {

		WordsTopicsTable wordsTopicsTable = new WordsTopicsTable();
		TopicsDocumentsTable topicsDocumentsTable = new TopicsDocumentsTable();

		// merge and concatenate
		for (LDAState ldaState : values) {
			// merge wordsTopicsTable
			wordsTopicsTable.mergeTables(ldaState.getWordsTopicsTable());

			// concatenate topicsDocumentsTable
			topicsDocumentsTable.concatenateTables(ldaState
					.getTopicsDocumentsTable());

			context.progress();
		}

		// sort topicsDocumentsTable
		topicsDocumentsTable.sortTable();

		context.write(key, new LDAState(wordsTopicsTable, topicsDocumentsTable));
	}
}
