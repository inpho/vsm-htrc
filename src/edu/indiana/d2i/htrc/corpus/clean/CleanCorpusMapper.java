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
package edu.indiana.d2i.htrc.corpus.clean;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

public class CleanCorpusMapper extends
		Mapper<Text, TextArrayWritable, Text, ArrayWritable> {

	enum HTRC_CORPUS {
		NUM_CLEANED_VOLUMES
	}

	@Override
	public void map(Text key, TextArrayWritable value, Context context)
			throws IOException, InterruptedException {
		// key is volume id, value is volume content (list of pages), key and
		// value are extracted from sequence file

		CorpusProcessingUtils.cleanVolume(value);

		context.write(key, value);

		// report progress
		context.getCounter(HTRC_CORPUS.NUM_CLEANED_VOLUMES).increment(1);
	}
}
