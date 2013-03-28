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
package edu.indiana.d2i.htrc;

public class Constants {
	// Data API related properties
	public static final String DATA_API_EPR = "data.api.epr";
	public static final String DATA_API_SELFSIGN = "data.api.selfsign";
	public static final String DATA_API_DELIMITER = "data.api.delimiter";
	public static final String DATA_API_VOL_PREFIX = "data.api.vol.prefix";
	public static final String DATA_API_PAGE_PREFIX = "data.api.page.prefix";
	public static final String DATA_API_CONCAT = "data.api.page.concat";
	public static final String DATA_API_REQ_SIZE = "data.api.req.size";
	public static final String DATA_API_DEFAULT_REQ_SIZE = "25";

	// Oauth2 related properties
	public static final String OAUTH2_EPR = "oauth2.epr";
	public static final String OAUTH2_USER_NAME = "oauth2.user.name";
	public static final String OAUTH2_USER_PASSWORD = "oauth2.user.password";

	// MapReduce related properties
	public static final String NUM_VOLUMES_PER_MAPPER = "num.volumes.per.mapper";

	// LDA analysis related properties
	public static final String LDA_ANALYSIS_MAX_ITER = "lda.analysis.max.iter";
	public static final String LDA_ANALYSIS_DEFAULT_MAX_ITER = "500";
	/* default step size to increase the capacity of topics-documents table */
	public static final String LDA_ANALYSIS_DEFAULT_STEP_SIZE = "50";
}
