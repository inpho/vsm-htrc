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
package edu.indiana.d2i.htrc.dataapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.amber.oauth2.client.OAuthClient;
import org.apache.amber.oauth2.client.URLConnectionClient;
import org.apache.amber.oauth2.client.request.OAuthClientRequest;
import org.apache.amber.oauth2.client.request.OAuthClientRequest.TokenRequestBuilder;
import org.apache.amber.oauth2.client.response.OAuthAccessTokenResponse;
import org.apache.amber.oauth2.common.exception.OAuthProblemException;
import org.apache.amber.oauth2.common.exception.OAuthSystemException;
import org.apache.amber.oauth2.common.message.types.GrantType;
import org.apache.amber.oauth2.common.utils.OAuthUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataAPIWrapper {
	private static final Log log = LogFactory.getLog(DataAPIWrapper.class);

	/* name indicating retrieving volume content as a whole */
	public static final String WHOLE_CONTENT = "WHOLE_CONTENT";
	/*
	 * name of error file in the zip stream when errors are encountered when
	 * retrieving volumes
	 */
	public static final String ERROR_FNAME = "ERROR.err";

	private String dataAPIEPR = null;
	private String dataAPIPagePrefix = null;
	private String dataAPIVolPrefix = null;
	private String delimiter = null;
	/* whether to concatenate all pages */
	private boolean isConcat = true;

	private String oauth2EPR = null;
	private String username = null;
	private String passwd = null;
	private boolean isSelfSigned = false;

	private String oauth2Token = null;

	private boolean isSecureConn = false;

	public DataAPIWrapper(String dataAPIEPR, String dataAPIPagePrefix,
			String dataAPIVolPrefix, String delimiter, boolean isConcat,
			String oauth2EPR, String username, String passwd,
			boolean isSelfSigned) throws KeyManagementException,
			NoSuchAlgorithmException, OAuthSystemException,
			OAuthProblemException {
		this.dataAPIEPR = dataAPIEPR;
		this.dataAPIPagePrefix = dataAPIPagePrefix;
		this.dataAPIVolPrefix = dataAPIVolPrefix;
		this.delimiter = delimiter;
		this.isConcat = isConcat;

		this.oauth2EPR = oauth2EPR;
		this.username = username;
		this.passwd = passwd;
		this.isSelfSigned = isSelfSigned;

		if (dataAPIEPR.startsWith("https"))
			isSecureConn = true;

		authenticate();
	}

	private void initSSL(boolean isSelfSigned) throws NoSuchAlgorithmException,
			KeyManagementException {
		if (isSelfSigned) {
			TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}

				public void checkClientTrusted(
						java.security.cert.X509Certificate[] certs,
						String authType) {
				}

				public void checkServerTrusted(
						java.security.cert.X509Certificate[] certs,
						String authType) {
				}

				@SuppressWarnings("unused")
				public boolean isServerTrusted(
						java.security.cert.X509Certificate[] certs) {
					return true;
				}

				@SuppressWarnings("unused")
				public boolean isClientTrusted(
						java.security.cert.X509Certificate[] certs) {
					return true;
				}
			} };

			SSLContext sslContext = SSLContext.getInstance("SSL");
			sslContext.init(null, trustAllCerts,
					new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sslContext
					.getSocketFactory());
		}
	}

	private void authenticate() throws NoSuchAlgorithmException,
			KeyManagementException, OAuthSystemException, OAuthProblemException {

		if (oauth2EPR.startsWith("https")) {

			initSSL(isSelfSigned);
		}

		TokenRequestBuilder tokenRequestBuilder = OAuthClientRequest
				.tokenLocation(oauth2EPR);
		tokenRequestBuilder.setGrantType(GrantType.CLIENT_CREDENTIALS);
		tokenRequestBuilder.setClientId(username);
		tokenRequestBuilder.setClientSecret(passwd);

		OAuthClientRequest clientRequest = tokenRequestBuilder
				.buildQueryMessage();

		OAuthClient client = new OAuthClient(new URLConnectionClient());

		OAuthAccessTokenResponse response = client.accessToken(clientRequest);

		oauth2Token = response.getAccessToken();
	}

	private String makePagesURL(Map<String, List<String>> pageIDs) {
		StringBuilder builder = new StringBuilder();

		// it's ok the last one has the delimiter
		for (String volId : pageIDs.keySet()) {
			builder.append(volId + "[");

			for (String pageNum : pageIDs.get(volId))
				builder.append(pageNum + ",");

			// delete the last ','
			builder.deleteCharAt(builder.length() - 1);
			builder.append("]" + delimiter);
		}

		if (isConcat)
			builder.append("&concat=true");

		return dataAPIEPR + dataAPIPagePrefix + builder.toString();
	}

	private String makeVolumesURL(List<String> volIDs) {
		StringBuilder builder = new StringBuilder();

		/**
		 * The last one has the delimiter will not cause any problem when
		 * &concat=true parameter doesn't appear, however, it will be treated as
		 * wrong syntax when the concat option is used, so we remove the last
		 * delimiter anyway
		 */
		for (String id : volIDs)
			builder.append(id + delimiter);

		// remove the last delimiter
		builder.deleteCharAt(builder.length() - 1);

		if (isConcat)
			builder.append("&concat=true");

		return dataAPIEPR + dataAPIVolPrefix + builder.toString();
	}

	public long retrievePageContents(
			Map<String, List<String>> volPageSequences,
			Map<String, Map<String, String>> volPageContents) {
		// TODO Auto-generated method stub

		String requestURL = makePagesURL(volPageSequences);

		if (log.isDebugEnabled())
			log.debug("requestURL = " + requestURL);

		return retrieveContents(requestURL, volPageContents);
	}

	private static void openZipStream(InputStream inputStream,
			Map<String, Map<String, String>> volPageContents, boolean isConcat)
			throws IOException {
		ZipInputStream zipInputStream = new ZipInputStream(inputStream);
		ZipEntry zipEntry = null;
		String currentVolID = null;

		if (!isConcat) {
			while ((zipEntry = zipInputStream.getNextEntry()) != null) {
				String name = zipEntry.getName();

				/**
				 * check whether encounter ERROR.err
				 */
				if (ERROR_FNAME.equals(name)) {
					log.error("Encountered ERROR.err file in the zip stream");
					// log the content of ERROR.err file
					log.error("*************** Start of Content of ERROR.err ***************");
					StringBuilder sb = new StringBuilder();
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(zipInputStream));
					String line = null;
					while ((line = reader.readLine()) != null)
						sb.append(line + "\n");

					log.error(sb.toString());
					log.error("*************** End of Content of ERROR.err ***************");

					continue;
				}

				if (zipEntry.isDirectory()) {
					// get volume name
					currentVolID = name.split("/")[0];
					volPageContents.put(currentVolID,
							new HashMap<String, String>());

					if (log.isDebugEnabled())
						log.debug("Encounter volueme directory : "
								+ currentVolID);

				} else {
					String[] names = name.split("/");

					// each page is a separate entry
//					assert names.length == 2;
//					assert names[0].equals(currentVolID);

					// get page(s) content
					StringBuilder sb = new StringBuilder();
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(zipInputStream,
									Charset.forName("UTF-8")));
					String line = null;
					while ((line = reader.readLine()) != null)
						sb.append(line + "\n");

					Map<String, String> contents = volPageContents
							.get(currentVolID);

					int idx = names[1].indexOf(".txt");
//					assert idx != -1;

					contents.put(names[1].substring(0, idx), sb.toString());
				}
			}
		} else {
			while ((zipEntry = zipInputStream.getNextEntry()) != null) {
				assert !zipEntry.isDirectory();
				String name = zipEntry.getName();

				/**
				 * check whether encounter ERROR.err
				 */
				if (ERROR_FNAME.equals(name)) {
					log.error("Encountered ERROR.err file in the zip stream");
					// log the content of ERROR.err file
					log.error("*************** Start of Content of ERROR.err ***************");
					StringBuilder sb = new StringBuilder();
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(zipInputStream));
					String line = null;
					while ((line = reader.readLine()) != null)
						sb.append(line + "\n");

					log.error(sb.toString());
					log.error("*************** End of Content of ERROR.err ***************");

					continue;
				}

				int idx = name.indexOf(".txt");
//				assert idx != -1;
				String cleanedVolId = name.substring(0, idx);

				if (log.isDebugEnabled())
					log.debug("Encounter volueme whole entry : " + cleanedVolId);

				StringBuilder sb = new StringBuilder();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(zipInputStream,
								Charset.forName("UTF-8")));
				String line = null;
				while ((line = reader.readLine()) != null)
					sb.append(line + "\n");

				HashMap<String, String> concatContent = new HashMap<String, String>();
				concatContent.put(DataAPIWrapper.WHOLE_CONTENT, sb.toString());
				volPageContents.put(cleanedVolId, concatContent);
			}
		}
	}

	public long retrieveVolContents(List<String> volumeIDs,
			Map<String, Map<String, String>> volPageContents, boolean isRandom) {
		// TODO Auto-generated method stub

		String requestURL = makeVolumesURL(volumeIDs);

		if (log.isDebugEnabled())
			log.debug("requestURL = " + requestURL);

		return retrieveContents(requestURL, volPageContents);
	}

	private long retrieveContents(String requestURL,
			Map<String, Map<String, String>> volPageContents) {
		long totalTime = 0;
		long startTime = 0;
		long endTime = 0;

		InputStream inputStream = null;

		try {

			if (isSecureConn)
				initSSL(isSelfSigned);

			startTime = System.currentTimeMillis();
			URL url = new URL(requestURL);
			URLConnection connection = url.openConnection();

			connection.addRequestProperty("Authorization", "Bearer "
					+ oauth2Token);

			if (isSecureConn)
				assert connection instanceof HttpsURLConnection;
			else
				assert connection instanceof HttpURLConnection;

			HttpURLConnection httpURLConnection = (HttpURLConnection) connection;
			httpURLConnection.setRequestMethod("GET");

			if (httpURLConnection.getResponseCode() == 200) {
				inputStream = httpURLConnection.getInputStream();
				openZipStream(inputStream, volPageContents, isConcat);

				endTime = System.currentTimeMillis();
				totalTime = endTime - startTime;

			} else {
				int responseCode = httpURLConnection.getResponseCode();
				log.error("Server response code : " + responseCode);
				inputStream = httpURLConnection.getInputStream();
				String responseBody = OAuthUtils
						.saveStreamAsString(inputStream);

				log.error(responseBody);

				/**
				 * Currently server doesn't provide a way to renew the token, or
				 * there is no response code particularly for the token
				 * expiration, 500 will be returned in this case, but it can
				 * also indicate other sorts of errors. In the following code we
				 * just renew the token anyway
				 * 
				 */
				if (responseCode == 500)
					authenticate();

			}

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			log.error("MalformedURLException : " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("IOException : " + e.getMessage());
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			log.error("KeyManagementException : " + e.getMessage());
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			log.error("NoSuchAlgorithmException : " + e.getMessage());
			e.printStackTrace();
		} catch (OAuthSystemException e) {
			// TODO Auto-generated catch block
			log.error("OAuthSystemException : " + e.getMessage());
			e.printStackTrace();
		} catch (OAuthProblemException e) {
			// TODO Auto-generated catch block
			log.error("OAuthProblemException : " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error("Failed to close zip inputstream");
				log.error("IOException : " + e.getMessage());
				e.printStackTrace();
			}
		}

		return totalTime;
	}
}
