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
package edu.indiana.d2i.htrc.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.indiana.d2i.htrc.corpus.analysis.LDAAnalysisDriver;

public class TestSuite {

	public static void testCommandLineParser(String[] args) throws IOException {
		GenericOptionsParser parser = new GenericOptionsParser(
				new Configuration(), args);

		CommandLine commandLine = parser.getCommandLine();

		Option[] options = commandLine.getOptions();

		for (Option op : options) {
			System.out.println(String.format("optName=%s, optValue=%s",
					op.getOpt(), op.getValue()));
		}

		// second application argument is the output folder prefix
		String[] appArgs = parser.getRemainingArgs();

		for (String appArg : appArgs) {
			System.out.println(String.format("appArg=%s", appArg));
		}

		int iterationNum = 0;

		String[] arguments = LDAAnalysisDriver
				.generateArgs(options, new String[0], appArgs[0], appArgs[1]
						+ "-iter-" + iterationNum);

		System.out.println("Passed in arguments in the first iteration");
		for (String arg : arguments) {
			System.out.println(String.format("argument=%s", arg));
		}

		String[] otherOps = {
				"-D",
				"user.args.lda.state.filepath=" + appArgs[1] + "-iter-"
						+ iterationNum + File.separator + "part-r-00000" };

		arguments = LDAAnalysisDriver.generateArgs(options, otherOps,
				appArgs[0], appArgs[1] + "-iter-" + ++iterationNum);

		System.out.println("Passed in arguments in the second iteration");
		for (String arg : arguments) {
			System.out.println(String.format("argument=%s", arg));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String[] commandLineArgs = {
				"-D",
				"user.args.mapping.table.filename=mapping_table",
				"-D",
				"user.args.topics.filename=topics",
				"-D",
				"user.args.topdoctable.capacity.stepsize=50",
				"-libjars",
				"/home/gruan/hadoop-1.0.4/hadoop-tools-1.0.4.jar,/home/gruan/hadoop-1.0.4/hadoop-client-1.0.4.jar",
				"-files",
				"/home/gruan/tmp/mapping_table,/home/gruan/tmp/topics",
				"corpus-input", "lda-analysis" };

		testCommandLineParser(commandLineArgs);
	}

}
