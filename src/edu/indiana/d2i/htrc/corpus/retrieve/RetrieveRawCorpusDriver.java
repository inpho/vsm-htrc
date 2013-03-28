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
package edu.indiana.d2i.htrc.corpus.retrieve;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.Constants;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;

public class RetrieveRawCorpusDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		/**
		 * Specify the # of reducers through -D
		 * mapred.reduce.tasks=<numOfReducers> in hadoop command line. Specify
		 * using compression through -D user.args.compression=true
		 */
		if (args.length != 3) {
			System.err
					.printf("Usage: %s [generic options] </path/to/input/directory> </path/to/output/directory> </path/to/property/file>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();

		Properties prop = new Properties();

		prop.load(new FileInputStream(args[2]));

		// set configuration parameters

		// data api related parameters
		conf.set(Constants.DATA_API_EPR,
				prop.getProperty(Constants.DATA_API_EPR));
		conf.set(Constants.DATA_API_CONCAT,
				prop.getProperty(Constants.DATA_API_CONCAT));
		conf.set(Constants.DATA_API_SELFSIGN,
				prop.getProperty(Constants.DATA_API_SELFSIGN));
		conf.set(Constants.DATA_API_DELIMITER,
				prop.getProperty(Constants.DATA_API_DELIMITER));
		conf.set(Constants.DATA_API_VOL_PREFIX,
				prop.getProperty(Constants.DATA_API_VOL_PREFIX));
		conf.set(Constants.DATA_API_PAGE_PREFIX,
				prop.getProperty(Constants.DATA_API_PAGE_PREFIX));
		conf.set(Constants.DATA_API_REQ_SIZE,
				prop.getProperty(Constants.DATA_API_REQ_SIZE));

		// oauth2 related parameters
		conf.set(Constants.OAUTH2_EPR, prop.getProperty(Constants.OAUTH2_EPR));
		conf.set(Constants.OAUTH2_USER_NAME,
				prop.getProperty(Constants.OAUTH2_USER_NAME));
		conf.set(Constants.OAUTH2_USER_PASSWORD,
				prop.getProperty(Constants.OAUTH2_USER_PASSWORD));

		// set # of lines (volumes in our case) to be processed by one map task
		conf.set("mapreduce.input.lineinputformat.linespermap",
				prop.getProperty(Constants.NUM_VOLUMES_PER_MAPPER));

		Job job = new Job(conf, "HTRC Retrieving Raw Corpus");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(RetrieveRawCorpusDriver.class);
		job.setMapperClass(RetrieveRawCorpusMapper.class);
		job.setReducerClass(RetrieveRawCorpusReducer.class);

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

		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RetrieveRawCorpusDriver(), args);
		System.exit(exitCode);
	}

}
