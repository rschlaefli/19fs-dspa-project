package ch.ethz.infk.dspa.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import ch.ethz.infk.dspa.helper.tuple.NTuple;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;

public class StaticDataParser {
	public static Stream<Tuple> parseCsvFile(String file) throws Exception {
		return parseCsvFile(file, null);
	}

	public static Stream<Tuple> parseCsvFile(
			String file, List<String> header) throws IOException {
		List<Tuple> result = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			List<String> headerRow;
			if (header != null) {
				// skip the header
				br.readLine();
				headerRow = header;
			} else {
				// extract the first line of the file (header)
				List<String> headerParts = Arrays.asList(br.readLine().split("\\|"));

				// ensure that there are no duplicate headers by appending unique suffixes
				// see https://stackoverflow.com/questions/40816421/find-duplicate-strings-in-list-and-make-them-unique
				Set<String> headerSet = new LinkedHashSet<>();
				for (String str : headerParts) {
					String value = str;
					// Iterate as long as you can't add the value indicating that we have already the value in the set
					for (int i = 2; !headerSet.add(value); i++) {
						value = str + "." + i;
					}
				}
				headerRow = new ArrayList<>(headerSet);
			}

			String line;
			while ((line = br.readLine()) != null && StringUtils.isNotEmpty(line)) {
				List<String> parts = Arrays.asList(line.split("\\|"));
				Tuple tuple = NTuple.newInstance(headerRow, parts);
				result.add(tuple);
			}
		}

		return result.stream();
	}
}
