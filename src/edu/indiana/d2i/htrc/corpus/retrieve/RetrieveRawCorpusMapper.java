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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.amber.oauth2.common.exception.OAuthProblemException;
import org.apache.amber.oauth2.common.exception.OAuthSystemException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.indiana.d2i.htrc.Constants;
import edu.indiana.d2i.htrc.corpus.CorpusProcessingUtils;
import edu.indiana.d2i.htrc.corpus.TextArrayWritable;
import edu.indiana.d2i.htrc.dataapi.DataAPIWrapper;

public class RetrieveRawCorpusMapper extends
		Mapper<LongWritable, Text, Text, TextArrayWritable> {

	enum HTRC_CORPUS {
		NUM_RETRIEVED_VOLUMES
	}

	private static final Log logger = LogFactory
			.getLog(RetrieveRawCorpusMapper.class);
	private DataAPIWrapper dataAPIAgent;
	private int numVolsPerReq;

	private List<String> vols = new ArrayList<String>();
	/**
	 * <volume_id, <"WHOLE_CONTENT", whole_volume_content>> when concat=true OR
	 * <volume_id, <page_id, page_content>> when concat=false
	 */
	private Map<String, Map<String, String>> volPageContents = new HashMap<String, Map<String, String>>();

	/* volume id */
	private Text key = new Text();
	/* volume content, in form of list of pages */
	private TextArrayWritable value = new TextArrayWritable();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();

		boolean success = false;
		try {
			dataAPIAgent = new DataAPIWrapper(conf.get(Constants.DATA_API_EPR),
					conf.get(Constants.DATA_API_PAGE_PREFIX),
					conf.get(Constants.DATA_API_VOL_PREFIX),
					conf.get(Constants.DATA_API_DELIMITER),
					Boolean.parseBoolean(conf.get(Constants.DATA_API_CONCAT)),
					conf.get(Constants.OAUTH2_EPR),
					conf.get(Constants.OAUTH2_USER_NAME),
					conf.get(Constants.OAUTH2_USER_PASSWORD),
					Boolean.parseBoolean(conf.get(Constants.DATA_API_SELFSIGN)));

			success = true;
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("KeyManagementException : " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("NoSuchAlgorithmException : " + e.getMessage());
		} catch (OAuthSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("OAuthSystemException : " + e.getMessage());
		} catch (OAuthProblemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("OAuthProblemException : " + e.getMessage());
		}

		if (!success) {
			System.err.println("Failed to instantiate dataAPI agent");
			logger.error("Failed to instantiate dataAPI agent");
			System.exit(-1);
		}

		// # of volumes per request
		numVolsPerReq = Integer.parseInt(conf.get(Constants.DATA_API_REQ_SIZE,
				Constants.DATA_API_DEFAULT_REQ_SIZE));
	}

	private void processVolumes(Context context) throws IOException,
			InterruptedException {

		if (vols.size() > 0) {

			dataAPIAgent.retrieveVolContents(vols, volPageContents, false);

			for (Map.Entry<String, Map<String, String>> volume : volPageContents
					.entrySet()) {

				/* set volume id */
				key.set(volume.getKey());

				Text[] pages = new Text[volume.getValue().size()];

				/**
				 * sort map by page order, e.g., 000001, 000002,...
				 */
				Map<String, String> sotred = CorpusProcessingUtils
						.sortPages(volume.getValue());

				int idx = 0;
				for (Map.Entry<String, String> page : sotred.entrySet()) {

					pages[idx++] = new Text(page.getValue());

				}

				/* set volume content in form of a list of pages */
				value.set(pages);
				context.write(key, value);
			}

			// report progress
			context.getCounter(HTRC_CORPUS.NUM_RETRIEVED_VOLUMES).increment(
					vols.size());

			// release space
			vols.clear();
			volPageContents.clear();
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// processing remaining volumes
		processVolumes(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// key is the line offset, not relevant here
		// value is volume id

		vols.add(value.toString());

		// Invoke data API to access the volume
		if (vols.size() >= numVolsPerReq) {
			processVolumes(context);
		}
	}
}
