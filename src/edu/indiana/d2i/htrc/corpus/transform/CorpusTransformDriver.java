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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

public class CorpusTransformDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		/**
		 * Specify the # of reducers through -D
		 * mapred.reduce.tasks=<numOfReducers> in hadoop command line. Specify
		 * whether compression is used through -D user.args.compression=true,
		 * use -D user.args.wordset.filename=<wordset_filename> to set wordset
		 * filename, use -files </local/path/to/wordset_file> to distribute
		 * wordset_file to each compute node
		 */
		if (args.length != 2) {
			System.err
					.printf("Usage: %s [generic options] </path/to/input/directory> </path/to/output/directory>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();

		Job job = new Job(conf, "HTRC Transforming Corpus");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(CorpusTransformDriver.class);
		job.setMapperClass(CorpusTransformMapper.class);
		job.setReducerClass(CorpusTransformReducer.class);

		if (conf.getBoolean("user.args.compression", false)) {
			/* use compression */
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job,
					CompressionType.BLOCK);
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					DefaultCodec.class);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CorpusTransformDriver(), args);
		System.exit(exitCode);
	}

}
