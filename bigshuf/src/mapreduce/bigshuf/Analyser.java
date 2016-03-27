package mapreduce.bigshuf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Analyser {

	private static final String COMBO_FREQUENCY_TABLE = "/home/mark/Desktop/Big_data/output/part-r-00000";
	private static final String LETTER_FREQUENCY_TABLE = "/home/mark/Desktop/Big_data/totals/part-r-00000";
	private static final String LETTER_FREQUENCY = "/home/mark/Desktop/Big_data/wordcount/part-r-00000";
	private static final String VOWEL_CONSONANT_FREQUENCY = "/home/mark/Desktop/Big_data/vowels/part-r-00000";
	private static final String DIFF_LETTER = "/home/mark/Desktop/Big_data/diff/part-r-00000";
	private Double totalLetters = 0.0;

	private static HashMap<String, Double> comboFrequency = new HashMap<>();
	private static HashMap<String, Double> letterFrequency = new HashMap<>();
	private HashMap<String, Double> frequencyPerLetter = new HashMap<>();
	private static HashMap<String, Double> vowelsConsonant = new HashMap<>();
	private static HashMap<String, Double> differentLetters = new HashMap<>();

	private final static Double avgLength = 5.0;

	private static String vowels = "eyuoia";

	public Analyser() throws IOException {
		File f = new File(COMBO_FREQUENCY_TABLE);
		BufferedReader br = new BufferedReader(new FileReader(f));
		Scanner sc;
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			try {
				sc = new Scanner(line);
				sc.useDelimiter("=");
				while (sc.hasNext()) {

					String key = sc.next();
					Double ammount = Double.parseDouble(sc.next());
					comboFrequency.put(key.toLowerCase(), ammount);
				}
			} catch (Exception e) {

			}
		}

		f = new File(LETTER_FREQUENCY_TABLE);
		br = new BufferedReader(new FileReader(f));
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			sc = new Scanner(line);
			sc.useDelimiter("=");
			while (sc.hasNext()) {
				String key = sc.next();
				Double ammount = Double.parseDouble(sc.next());
				letterFrequency.put(key.toLowerCase(), ammount);
			}
		}

		f = new File(LETTER_FREQUENCY);
		br = new BufferedReader(new FileReader(f));
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			sc = new Scanner(line);
			sc.useDelimiter("=");
			while (sc.hasNext()) {
				String key = sc.next();
				Double ammount = Double.parseDouble(sc.next());
				frequencyPerLetter.put(key.toLowerCase(), ammount);
				totalLetters += ammount;
			}
		}

		f = new File(VOWEL_CONSONANT_FREQUENCY);
		br = new BufferedReader(new FileReader(f));
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			sc = new Scanner(line);
			sc.useDelimiter("=");
			while (sc.hasNext()) {
				String key = sc.next();
				Double ammount = Double.parseDouble(sc.next());
				vowelsConsonant.put(key.toLowerCase(), ammount);
			}
		}

		f = new File(DIFF_LETTER);
		br = new BufferedReader(new FileReader(f));
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			sc = new Scanner(line);
			sc.useDelimiter("=");
			while (sc.hasNext()) {
				String key = sc.next();
				Double ammount = Double.parseDouble(sc.next());
				differentLetters.put(key.toLowerCase(), ammount);
			}
		}
	}

	public static void loadData(String filename, HashMap<String, Double> dest) {

		try {
			File file = new File(filename);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
			Scanner scanner;

			for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
				scanner = new Scanner(line);
				scanner.useDelimiter("=");
				while (scanner.hasNext()) {
					String key = scanner.next();
					Double ammount = Double.parseDouble(scanner.next());
					dest.put(key.toLowerCase(), ammount);
				}
			}
		} catch (IOException e) {

		}

	}

	public static Double predict(String word) {
		boolean isBigram = false;
		Double chance = 0.0;

		isBigram = performWordChecks(word, isBigram);
		chance = performChanceCalculations(word, isBigram, chance);

		return chance;
	}

	private static Double performChanceCalculations(String word, boolean isBigram, Double chance) {
		chance = calculateBigramChance(word, chance);

		if (!isBigram) {
			// This feature checks if the first letter of the word is a
			// consonent and the last letter a vowel.
			chance = procesConsonentVowel(word, chance);
			chance = calculateLetterAverage(word, chance);
			chance = calculateBigramDifference(word, chance);
		}
		return chance;
	}

	private static boolean performWordChecks(String word, boolean isBigram) {
		checkIfWordIsAllowed(word);
		isBigram = checkIfWordIsBigram(word, isBigram);
		return isBigram;
	}

	private static boolean checkIfWordIsBigram(String word, boolean isBigram) {
		if (word.length() == 2) {
			isBigram = true;
		}
		return isBigram;
	}

	private static Double checkIfWordIsAllowed(String word) {
		// if 1 letter allow
		if (word.length() == 1) {
			return 1.0;
		}
		return 0.0;
	}

	private static Double calculateBigramChance(String word, Double chance) {
		for (int i = 0; i < word.length(); i++) {
			try {
				if (calculateChanceofLetterSequence(word.charAt(i), word.charAt(i + 1)) < 0.025) {
					chance = ComplexPredict(word);
				} else {
					chance = 1.0;
				}
			} catch (StringIndexOutOfBoundsException e) {

			}

		}

		// feature 2 , do the same in reverse
		StringBuffer r = new StringBuffer();
		r.reverse();
		String reversed = r.toString();

		for (int i = 0; i < reversed.length(); i++) {
			try {
				if (calculateChanceofLetterSequence(reversed.charAt(i), reversed.charAt(i + 1)) < 0.25) {
					chance = ComplexPredict(word);
				} else {
					chance = 1.0;
				}
			} catch (StringIndexOutOfBoundsException e) {

			}

		}
		return chance;
	}

	private static Double procesConsonentVowel(String word, Double chance) {
		try {

			Double divisionFactor = (vowelsConsonant.get("cv") / vowelsConsonant.get("vc"));
			if (!firstIsVowel(word.toLowerCase().charAt(0) + "")
					&& lastIsVowel(word.toLowerCase().charAt(word.length() - 1) + "")) {
				chance = chance / divisionFactor;
			}

		} catch (Exception e) {

		}
		return chance;
	}

	private static Double calculateLetterAverage(String word, Double chance) {
		Double difference = word.length() - avgLength;
		if (difference < 0) {
			difference = -difference;
		}
		if (difference != 0) {
			chance = chance / difference;
		}
		return chance;
	}

	private static Double calculateBigramDifference(String word, Double chance) {
		Double average = differentLetters.get("avg");
		Double currentAmmunt = ammountOfDifferentLetters(word);
		Double differenceLetters = average - currentAmmunt;

		if (differenceLetters < 0) {
			differenceLetters = -differenceLetters;
		}

		if (differenceLetters != 0) {
			chance = chance / differenceLetters;
		}
		return chance;
	}

	public static Double ComplexPredict(String word) {

		Double divisors = 0.0;
		Double sum = 0.0;
		for (int i = 0; i < word.length(); i++) {

			try {
				sum += calculateChanceofLetterSequence(word.charAt(i), word.charAt(i + 1));
			} catch (StringIndexOutOfBoundsException e) {

			}
			divisors++;
		}

		// System.err.println(sum / divisors);
		return sum / divisors;
	}

	public static Double calculateChanceofLetterSequence(char first, char second) {

		Double events = comboFrequency.get(first + "_" + second);
		Double maxPossible = letterFrequency.get(first + "");

		if (events == null) {
			return 0.0;
		}
		return events / maxPossible;

	}

	public Double calclulateChanceOfLetterOccurance(char letter) {
		Double chance = 0.0;
		Double occuranceSelectedLetter = frequencyPerLetter.get(letter + "");
		chance = occuranceSelectedLetter / totalLetters;
		return chance;

	}

	public static boolean firstIsVowel(String word) {
		return vowels.contains(word.charAt(0) + "") ? true : false;
	}

	public static boolean lastIsVowel(String word) {
		return vowels.contains(word.charAt(word.length() - 1) + "") ? true : false;
	}

	public static Double ammountOfDifferentLetters(String word) {
		ArrayList<Character> differentLetter = new ArrayList<>();

		for (int i = 0; i < word.length(); i++) {
			char current = word.charAt(i);
			if (!differentLetter.contains(current)) {
				differentLetter.add(current);
			}
		}

		return (double) differentLetter.size();
	}

	public static Double predictSentence(String sentence) {

		String[] words = sentence.split(" ");

		Double chance = 0.0;
		Double sum = 0.0;
		Double divisors = 0.0;
		for (String word : words) {
			sum += predict(word);
			divisors++;
		}
		chance = sum / divisors;
		return chance;
	}
}