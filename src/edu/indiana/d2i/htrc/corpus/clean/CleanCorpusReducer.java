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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

/**
 * Identity Reducer, actually can be omitted
 * 
 * @author Guangchen Ruan
 * 
 */
public class CleanCorpusReducer extends
		Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<TextArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		/* key is volume id, value is pages */

		/**
		 * The iterator should only have a single element
		 */
		for (TextArrayWritable pages : values) {
			context.write(key, pages);
			context.progress();
			break;
		}

	}
}
