package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AmicableReducer extends Reducer<Text, Text, Text, Text> {

	private Scanner scanner;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String outputResult = "";
		for (Text t : values) {
			outputResult += t.toString();
		}

		HashMap<Integer, Integer> recon = new HashMap<>();

		scanner = new Scanner(outputResult);
		scanner.useDelimiter(" ");

		while (scanner.hasNext()) {
			recon.put(scanner.nextInt(), scanner.nextInt());
		}

		for (Integer sleutel : recon.keySet()) {
			int huidigeWaarde = recon.get(sleutel);
			for (Integer i : recon.keySet()) {
				if (huidigeWaarde == i && i != sleutel) {
					context.write(new Text("PAIR"), new Text("" + i + " - " + sleutel));
				}

			}
		}

	}

}
