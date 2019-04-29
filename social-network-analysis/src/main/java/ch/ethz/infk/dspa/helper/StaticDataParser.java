package ch.ethz.infk.dspa.helper;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StaticDataParser {
    public static <T extends Tuple> Stream<Tuple> parseCsvFile(String file) throws IOException {
        List<Tuple> result = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            // skip header
            br.readLine();

            String line;
            while ((line = br.readLine()) != null && StringUtils.isNotEmpty(line)) {
                String[] parts = line.split("\\|");
                Tuple tuple = T.newInstance(parts.length);
                for (int i = 0; i < parts.length; i++) {
                    tuple.setField(parts[i], i);
                }
                result.add(tuple);
            }
        }

        return result.stream();
    }
}
