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
package edu.indiana.d2i.htrc.corpus.transform;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

public class CorpusTransformMapper extends
		Mapper<Text, TextArrayWritable, Text, TextArrayWritable> {

	enum HTRC_CORPUS {
		NUM_TRANSFORMED_VOLUMES
	}

	private List<String> wordSet = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		/**
		 * word set file is distributed to each node via distributed cache
		 * mechanism
		 */
		Configuration conf = context.getConfiguration();
		String wordSetFileName = conf.get("user.args.wordset.filename");

		BufferedReader reader = null;
		String line = null;

		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(wordSetFileName)));

			/* each line is a word */
			while ((line = reader.readLine()) != null)
				wordSet.add(line.trim());

		} finally {
			if (reader != null)
				reader.close();
		}
	}

	@Override
	public void map(Text key, TextArrayWritable value, Context context)
			throws IOException, InterruptedException {
		// key is volume id, value is volume content (list of pages), key and
		// value are extracted from sequence file

		CorpusProcessingUtils.transformVolume(value, wordSet);

		context.write(key, value);

		// report progress
		context.getCounter(HTRC_CORPUS.NUM_TRANSFORMED_VOLUMES).increment(1);
	}
}