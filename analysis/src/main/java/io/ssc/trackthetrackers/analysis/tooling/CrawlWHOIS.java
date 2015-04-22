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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flink.shaded.com.google.common.net.InternetDomainName;

// Extract domains' companies incrementally every X seconds via WHOISComapnyLookup.java
// Input format:
// google.com \n facebook.com \n yahoo.com \n
// Output: google.com, Google Inc. \n facebook.com, Facebook, Inc. \n
//
// If encountering "Connection reset", it's due to killing the current connection because it takes too long 
// mainly due to connection refused or irregular whois response.
// If encountering "Connection refused", this process needs to wait for days.
// If encountering "No such element", that whois response doesn't contain administrative organization information.

/**
 * ICANN term of use: All results shown are captured from registries and/or
 * registrars and are framed in real-time. ICANN does not generate, collect,
 * retain or store the results shown other than for the transitory duration
 * necessary to show these results in response to real-time queries.* These
 * results are shown for the sole purpose of assisting you in obtaining
 * information about domain name registration records and for no other purpose.
 * You agree to use this data only for lawful purposes and further agree not to
 * use this data (i) to allow, enable, or otherwise support the transmission by
 * email, telephone, or facsimile of mass unsolicited, commercial advertising,
 * or (ii) to enable high volume, automated, electronic processes to collect or
 * compile this data for any purpose, including without limitation mining this
 * data for your own personal or commercial purposes. ICANN reserves the right
 * to restrict or terminate your access to the data if you fail to abide by
 * these terms of use. ICANN reserves the right to modify these terms at any
 * time. By submitting a query, you agree to abide by these terms. See
 * http://whois.icann.org/
 */
public class CrawlWHOIS {

	// Every x MILLISECONDS get WHOIS once
	private static int WHOISdelay = 50;

	// If lookup takes too long, it's mainly due to connection refused or
	// irregular whois response, so kill this lookup and continue to the next
	// lookup
	private static int WHOISTimeout = 2;

	private static String domainComapnyPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/DomainAndCompany.csv";
	private static String domainLookupPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/TopDomains";
	private static String exceptionDomainPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/ExceptionDomain.csv";
	private static FileReader fileReader;
	private static BufferedReader bufferedReader;

	private static boolean INCREMENTAL_EXTRACTION;

	private static HashMap<String, String> domainCompanyMap;
	private static HashMap<String, String> domainKnownMap;
	private static HashSet<String> domainCheckingSet;
	private static HashSet<String> exceptionDomainSet;

	private static String company = "N/A";

	public static void main(String args[]) throws Exception {

		// Incremental check: Check if the domain already processed before
		INCREMENTAL_EXTRACTION = checkProcessBefore(domainComapnyPath);

		domainKnownMap = new HashMap<String, String>();
		// Prepare the domains want to check
		domainCheckingSet = new HashSet<String>();
		domainCheckingSet = readDomain(domainLookupPath);

		if (INCREMENTAL_EXTRACTION) {
			domainKnownMap = readDomainKnown(domainComapnyPath);
			exceptionDomainSet = readDomain(exceptionDomainPath);
			domainCheckingSet.removeAll(domainKnownMap.keySet());
			domainCheckingSet.removeAll(exceptionDomainSet);
		}

		System.out.println("domains lookup: " + domainCheckingSet.size()
				+ " takes " + (int) domainCheckingSet.size() * 2 * WHOISdelay
				/ 6000 + " minutes maximum");

		domainCompanyMap = new HashMap<String, String>();

		for (String domain : domainCheckingSet) {			
			// Whois query needs top domain
			// If not topDomain, using top domain get WHOIS data
			String topDomain = InternetDomainName.from(domain)
					.topPrivateDomain().toString();

			String tld = domain.substring(domain.lastIndexOf(".") + 1).trim()
					.toLowerCase();

			// Possibly it's already known
			if (!topDomain.equalsIgnoreCase(domain)) {
				if (domainCompanyMap.containsKey(topDomain)) {
					company = domainCompanyMap.get(topDomain);
					domainCompanyMap.put(domain, company);
				} else if (domainKnownMap.containsKey(topDomain)) {
					company = domainKnownMap.get(topDomain);
					domainCompanyMap.put(domain, company);
				}
			}

			// If it's a ccTLD, check if it contains symbol (ex.
			// amazon.jp, google.fr), the length is 2
			else if (tld.length() < 3) {
				String checkingSymbol = domain.split("\\.")[0];

				Iterator<Entry<String, String>> iterator = domainKnownMap
						.entrySet().iterator();

				while (iterator.hasNext()) {
					Entry<String, String> currentEntry = iterator.next();
					String domainKnown = currentEntry.getKey();
					String companyKnown = currentEntry.getValue();

					String knownSymbols = domainKnown.split("\\.")[0];

					if (knownSymbols.equalsIgnoreCase(checkingSymbol))
						domainCompanyMap.put(domain, companyKnown);
				}
			}

			// Start WHOIS
			else {
				WHOISCompanyLookup whoisICANN = new WHOISCompanyLookup();
				exceptionDomainSet = new HashSet<String>();

				// If lookup takes too long, it's mainly due to connection
				// refused or irregular whois response,
				// so kill this lookup and continue to the next lookup
				final ExecutorService executorService = Executors
						.newSingleThreadExecutor();
				Future<?> future = null;
				try {
					future = executorService.submit(() -> {
						try {
							company = whoisICANN.getWHOIS(topDomain);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					});
					future.get(WHOISTimeout, TimeUnit.SECONDS);
				} catch (Exception e) {
					if (future != null) {
						future.cancel(true);
					}
				}

				if (!company.equalsIgnoreCase("N/A"))
					domainCompanyMap.put(domain, company);

				// Write the result if cannot get the company
				else {
					exceptionDomainSet.add(domain);
					writeCheckedDomainToDisk(domainCompanyMap,
							domainComapnyPath);
					writeExceptionDomainToDisk(exceptionDomainSet,
							exceptionDomainPath);
					System.out.println("Writting...");

					domainCompanyMap.clear();
					exceptionDomainSet.clear();

				}
				// Wait X MILLISECONDS
				TimeUnit.MILLISECONDS.sleep(WHOISdelay);
			}
		}

		writeCheckedDomainToDisk(domainCompanyMap, domainComapnyPath);
		System.out.println("End WHOIS Extraction");
	}

	// Write the result to disk
	public static void writeExceptionDomainToDisk(
			HashSet<String> excetptionDomainSet, String fileOutputPath)
			throws IOException {

		FileWriter fileWriter = new FileWriter(fileOutputPath, true);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Iterator<String> iterator = excetptionDomainSet.iterator();

		while (iterator.hasNext()) {
			String entry = iterator.next();
			bufferedWriter.write(entry + "\n");
		}

		bufferedWriter.close();
		fileWriter.close();
	}

	// Check if the output file already exist
	private static boolean checkProcessBefore(String knownDomainFilePath) {
		File domainknownFile = new File(knownDomainFilePath);
		if (domainknownFile.exists())
			return true;
		else
			return false;
	}

	// Prepare the domains already checked
	public static HashMap<String, String> readDomainKnown(
			String knownDomainFilePath) throws IOException {

		HashMap<String, String> domainKnownMap = new HashMap<String, String>();
		fileReader = new FileReader(knownDomainFilePath);
		bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();

		while (line != null) {
			String[] domainAndCompany = line.split(",");
			String domainKnown = domainAndCompany[0];
			String company = domainAndCompany[1];
			if (!company.equalsIgnoreCase("null"))
				domainKnownMap.put(domainKnown, company);
			line = bufferedReader.readLine();
		}

		bufferedReader.close();
		fileReader.close();

		return domainKnownMap;
	}

	// Read domain into a set
	public static HashSet<String> readDomain(String domainCheckingFilePath)
			throws IOException {

		HashSet<String> domainCheckingSet = new HashSet<String>();
		fileReader = new FileReader(domainCheckingFilePath);
		bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();

		while (line != null) {
			domainCheckingSet.add(line);
			line = bufferedReader.readLine();
		}

		bufferedReader.close();
		fileReader.close();

		return domainCheckingSet;
	}

	// Write the result to disk
	public static void writeCheckedDomainToDisk(
			HashMap<String, String> domainCompanyMap, String fileOutputPath)
			throws IOException {

		FileWriter fileWriter = new FileWriter(fileOutputPath, true);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<Entry<String, String>> iterator = domainCompanyMap.entrySet()
				.iterator();

		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			bufferedWriter
					.write(entry.getKey() + "," + entry.getValue() + "\n");
		}

		bufferedWriter.close();
		fileWriter.close();
	}
}
