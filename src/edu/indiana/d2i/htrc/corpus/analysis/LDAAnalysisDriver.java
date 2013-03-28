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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.htrc.Constants;
import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalyzer.LDAState;

public class LDAAnalysisDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		/**
		 * Following generic arguments should be specified in command line
		 * 
		 * -D user.args.mapping.table.filename=<mappingtable_filename> -D
		 * user.args.topics.filename=<topics_filename> -D
		 * user.args.topdoctable.capacity.stepsize=<stepSize> -D
		 * user.args.lda.state.filepath=</hdfs/path/to/lda/state/file> (being
		 * set automatically) -files
		 * </local/path/to/mapping/table/file>,</local/path/to/topics/file>
		 * -libjars <dependent jars> (if any)
		 * 
		 */
		if (args.length != 2) {
			System.err
					.printf("Usage: %s [generic options] </path/to/input/directory> </path/to/output/directory/prefix> <path/to/property/file>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();

		Job job = new Job(conf, "HTRC LDA Analysis");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(LDAAnalysisDriver.class);
		job.setMapperClass(LDAAnalysisMapper.class);
		job.setReducerClass(LDAAnalysisReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LDAState.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		/* set number of reduce tasks to be 1 */
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static String[] generateArgs(Option[] genericOptions,
			String[] remainingGenericOps, String... appArgs) {
		List<String> args = new ArrayList<String>();

		for (Option op : genericOptions) {
			args.add("-" + op.getOpt());
			args.add(op.getValue());
		}

		for (String s : remainingGenericOps) {
			args.add(s);
		}

		for (String arg : appArgs) {
			args.add(arg);
		}

		return args.toArray(new String[0]);
	}

	/**
	 * 
	 * @param ldaStateFilePath
	 *            : HDFS path pointing to lda state file
	 * @return
	 * @throws IOException
	 */
	private static boolean isAnalysisConverged(String ldaStateFilePath)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader seqFileReader = null;

		try {
			seqFileReader = new SequenceFile.Reader(fs, new Path(
					ldaStateFilePath), conf);

			Text key = (Text) ReflectionUtils.newInstance(
					seqFileReader.getKeyClass(), conf);
			LDAState ldaState = (LDAState) ReflectionUtils.newInstance(
					seqFileReader.getValueClass(), conf);

			// the sequence file should only have one record
			seqFileReader.next(key, ldaState);

			return LDAAnalyzer.isConverged(ldaState);

		} finally {
			IOUtils.closeStream(seqFileReader);
		}
	}

	public static void main(String[] args) throws Exception {

		GenericOptionsParser parser = new GenericOptionsParser(
				new Configuration(), args);

		CommandLine commandLine = parser.getCommandLine();

		Option[] options = commandLine.getOptions();

		/**
		 * appArgs[0] = <path/to/input/directory> (where sequence files reside)
		 * appArgs[1] = <path/to/output/directory/prefix> (where LDA state file
		 * should go) appArgs[2] = <path/local/property/file>
		 * 
		 * Note: the passed in <path/to/output/directory/prefix> is only a
		 * prefix, we automatically append the iteration number suffix
		 */
		String[] appArgs = parser.getRemainingArgs();

		// load property file
		Properties prop = new Properties();
		prop.load(new FileInputStream(appArgs[2]));

		int maxIterationNum = Integer.parseInt(prop.getProperty(
				Constants.LDA_ANALYSIS_MAX_ITER,
				Constants.LDA_ANALYSIS_DEFAULT_MAX_ITER));

		int iterationCount = 0;

		/**
		 * in the first iteration (iteration 0), there is no LDA state
		 */
		String[] arguments = generateArgs(options, new String[0], appArgs[0],
				appArgs[1] + "-iter-" + iterationCount);

		/**
		 * iterate until convergence or maximum iteration number reached
		 */
		while (true) {

			int exitCode = ToolRunner.run(new LDAAnalysisDriver(), arguments);

			System.out.println(String.format(
					"LDA analysis finished iteration %d, with exitCode = %d",
					iterationCount, exitCode));

			/**
			 * LDA state is the output (sequence file) from current iteration
			 * and is used to initialize the words-topics table and
			 * topics-documents table for the next iteration
			 */
			String ldaStateFilePath = appArgs[1] + "-iter-" + iterationCount
					+ File.separator + "part-r-00000";

			/**
			 * load LDA state to check whether it is converged
			 */
			if (isAnalysisConverged(ldaStateFilePath)) {
				System.out.println(String.format(
						"LDA analysis converged at iteration %d",
						iterationCount));
				break;
			}

			if ((iterationCount + 1) >= maxIterationNum) {
				System.out
						.println(String
								.format("LDA analysis reached the maximum iteration number %d, going to stop",
										maxIterationNum));
				break;
			}

			String[] otherOps = { "-D",
					"user.args.lda.state.filepath=" + ldaStateFilePath };

			/**
			 * generate arguments for the next iteration and increase iteration
			 * count
			 */
			arguments = generateArgs(options, otherOps, appArgs[0], appArgs[1]
					+ "-iter-" + ++iterationCount);
		}

	}
}
