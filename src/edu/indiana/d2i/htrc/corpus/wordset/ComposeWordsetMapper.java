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
package edu.indiana.d2i.htrc.corpus.wordset;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

public class ComposeWordsetMapper extends
		Mapper<Text, TextArrayWritable, Text, NullWritable> {

	enum HTRC_CORPUS {
		NUM_PROCESSED_VOLUMES
	}

	private Set<String> wordSet = new HashSet<String>();

	private Text key = new Text();
	private NullWritable value = NullWritable.get();

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// emit all words

		for (String word : wordSet) {
			key.set(word);
			context.write(key, value);
		}
	}

	@Override
	public void map(Text key, TextArrayWritable value, Context context)
			throws IOException, InterruptedException {
		// key is volume id, value is volume content (list of pages), key and
		// value are extracted from sequence file

		Set<String> words = CorpusProcessingUtils.getWordSet(value);

		/**
		 * Perform local aggregation instead of emit immediately to make
		 * processing more efficient
		 */
		wordSet.addAll(words);

		// report progress
		context.getCounter(HTRC_CORPUS.NUM_PROCESSED_VOLUMES).increment(1);
	}
}