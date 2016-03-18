package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AmicableMapper extends Mapper<LongWritable, Text, Text, Text> {

	private int inputNumber;
	private int amicableProduct;
	private ArrayList<Integer> indentityMap = new ArrayList<>();
	private final int add = 2;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split(" ");
		for (String s : line) {
			inputNumber = Integer.parseInt(s);

			if (!indentityMap.contains(inputNumber)) {
				amicableProduct = sumFactors(inputNumber);
				if (inputNumber % 2 != 0 && amicableProduct % 2 != 0 && sumFactors(amicableProduct) == inputNumber) {
					context.write(new Text("A"), new Text(inputNumber + " " + amicableProduct + " "));
				} else if (inputNumber % 2 == 0 && amicableProduct % 2 == 0
						&& sumFactors(amicableProduct) == inputNumber) {
					context.write(new Text("A"), new Text(inputNumber + " " + amicableProduct + " "));
				}
			}
		}
	}

	private int sumFactors(int inputNumber) {
		
		int maxDivisors = (int) Math.sqrt(inputNumber);
		int sum = 1;

		if(inputNumber % 2 == 0){
			sum+=2;
			sum+= inputNumber / 2;
		}
		
		for (int index = 3; index <= maxDivisors; index+=add) {
			if (inputNumber % index == 0) {
				sum += index;
				int divisor = inputNumber / index;
				if (divisor != index)
					sum += divisor;
			}
		}
		return sum;
	}
}
