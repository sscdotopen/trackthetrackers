/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Hung Chang
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.tooling;

import java.util.StringTokenizer;

import org.apache.commons.net.whois.WhoisClient;

// Connect to whois server and get the administrative organization information
/**
 * ICANN term of use:
 * All results shown are captured from registries and/or registrars and are
 * framed in real-time. ICANN does not generate, collect, retain or store the
 * results shown other than for the transitory duration necessary to show these
 * results in response to real-time queries.* These results are shown for the
 * sole purpose of assisting you in obtaining information about domain name
 * registration records and for no other purpose. You agree to use this data
 * only for lawful purposes and further agree not to use this data (i) to allow,
 * enable, or otherwise support the transmission by email, telephone, or
 * facsimile of mass unsolicited, commercial advertising, or (ii) to enable high
 * volume, automated, electronic processes to collect or compile this data for
 * any purpose, including without limitation mining this data for your own
 * personal or commercial purposes. ICANN reserves the right to restrict or
 * terminate your access to the data if you fail to abide by these terms of use.
 * ICANN reserves the right to modify these terms at any time. By submitting a
 * query, you agree to abide by these terms. See http://whois.icann.org/
 */

public class WHOISCompanyLookup {

	public static final String IANA_WHOIS_SERVER = "whois.iana.org";

	public static final int WHOIS_PORT = 43;

	public String getWHOIS(String host) throws Exception {

		// Get the address of the authoritative Whois server from IANA
		WhoisClient whoisClient = new WhoisClient();
		whoisClient.connect(IANA_WHOIS_SERVER, WHOIS_PORT);

		StringBuilder result = new StringBuilder("");
		String tmpStr = whoisClient.query(host);
		whoisClient.disconnect();

		result.append(tmpStr);
		int idx = tmpStr.indexOf("whois:");
		tmpStr = tmpStr.substring(idx + 6).trim();
		String actualServer = tmpStr.substring(0, tmpStr.indexOf("\n"));

		// Special-case some TLDs
		String tld = host.substring(host.lastIndexOf(".") + 1).trim()
				.toLowerCase();

		// Suppress Japanese characters in output
		if ("jp".equals(tld))
			host += "/e";

		// Get the actual Whois data
		whoisClient.connect(actualServer, WHOIS_PORT);
		// The prefix "domain " solves the problem with spurious server names
		// Domain names in the .com and .net domains can be registered
		// with many different competing registrars.
		// Go to http://www.internic.net for detailed information.
		// Ex. google registered in http://www.markmonitor.com,
		// so it becomes
		// 'whois -h whois.markmonitor.com google.com'
		// to get the WHOIS data.
		// (like for google.com, apple.com. yahoo.com, microsoft.com etc.)
		if ("com".equals(tld)) {
			tmpStr = whoisClient.query("domain " + host);
			result.append(tmpStr);
		} else if ("de".equals(tld)) {
			// The syntax for .de is slightly different.
			tmpStr = whoisClient.query("-T dn " + host);
			result.append(tmpStr);
		}

		else {
			tmpStr = whoisClient.query(host);
			result.append(tmpStr);
		}
		whoisClient.disconnect();
		// printResults(actualServer, tmpStr);

		// If there is a more specific server, query that one too
		idx = tmpStr.indexOf("Whois Server:");
		if (idx != -1) {
			tmpStr = tmpStr.substring(idx + 13).trim();
			actualServer = tmpStr.substring(0, tmpStr.indexOf("\n"));
			whoisClient.connect(actualServer, WHOIS_PORT);
			tmpStr = whoisClient.query(host);
			result.append(tmpStr);
			whoisClient.disconnect();
			// printResults(actualServer, tmpStr);
		}
		// System.out.println(result.toString());
		String adminOrg = getAdminOrgOfDomain(result.toString());

		// Irregular response or the organization information is hidden
		// (Admin: and Admin:null)
		if (adminOrg == null || adminOrg.isEmpty()
				|| adminOrg.equalsIgnoreCase("null")
				|| adminOrg.equalsIgnoreCase(",")) {
			// printResults(actualServer, tmpStr);
			return "N/A";
		}
		return adminOrg;
	}

	private static void printResults(String server, String results) {
		// Remove Windows-style line endings
		results = results.replaceAll("\r", "");
		System.out.println("\n\nFrom " + server + ":\n");
		System.out.println(results);
	}

	// Find the line in the raw text of WHOIS which is
	// Admin Organization : Organization Name
	private static String getAdminOrgOfDomain(String whoisResult) {
		String adminOrg = null;
		String organizationLine = null;
		StringTokenizer lineTokenizer = new StringTokenizer(
				whoisResult.toString(), "\n");
		
		while (lineTokenizer.hasMoreTokens()) {
			String line = lineTokenizer.nextToken();

			if (line.contains("Admin Organization")) {
				organizationLine = line;
				StringTokenizer colonTokenizer = new StringTokenizer(
						organizationLine, ":");
				colonTokenizer.nextToken();
				adminOrg = colonTokenizer.nextToken().trim();
			}
		}
		return adminOrg;
	}
}
