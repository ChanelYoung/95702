package ds;
//Work Cited: https://github.com/CMU-Heinz-95702/lab9-MapReduceAndSpark
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.Scanner;

/**
 * @goal      The ShakespeareAnalytics program performs various analytics tasks on the text of "All's Well That Ends Well" using Apache Spark.
 *            Tasks include counting lines, words, distinct words, symbols, distinct symbols, and distinct letters.
 *            Additionally, the program prompts the user to enter a word and displays all lines containing that word.
 * @author   Xinyuan Yang(xy3)
 * @email    xy3@andrew.cmu.edu
 */


public class ShakespeareAnalytics {

    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        // Number of lines
        long numberOfLines = inputFile.count();
        //Print Out
        System.out.println("Task 0: Display the number of lines in \"All's Well That Ends Well\".");
        System.out.println("Number of lines: " + numberOfLines);

        // Split the line into an array of words
        JavaRDD<String> WordCount = inputFile.flatMap(line -> Arrays.asList(line.split("[^a-zA-Z]+")));
        // Deal with the blank
        // Work Cited: https://spark.apache.org/docs/1.0.2/api/java/org/apache/spark/api/java/JavaRDD.html
        JavaRDD<String> KnockoutBlank = WordCount.filter(word -> !word.isEmpty());

        // Number of words
        long numberOfWords = KnockoutBlank.count();
        //Print Out
        System.out.println("Task 1: Display the number of words in \"All's Well That Ends Well\".");
        System.out.println("Number of words: " + numberOfWords);

        // Number of distinct words
        long numberOfDistinctWords = KnockoutBlank.distinct().count();
        //Print Out
        System.out.println("Task 2: Display the number of distinct words in \"All's Well That Ends Well\".");
        System.out.println("Number of distinct words: " + numberOfDistinctWords);

        // Number of symbols
        long numberOfSymbols = inputFile.flatMap(content -> Arrays.asList(content.split(""))).count();
        //Print Out
        System.out.println("Task 3: Find the number of symbols in \"All's Well That Ends Well\".");
        System.out.println("Number of symbols: " + numberOfSymbols);

        // Number of distinct symbols
        long numberOfDistinctSymbols = inputFile.flatMap(content -> Arrays.asList(content.split("")))
                .distinct().
                count();
        //Print Out
        System.out.println("Task 4: Find the number of distinct symbols in \"All's Well That Ends Well\".");
        System.out.println("Number of distinct symbols: " + numberOfDistinctSymbols);

        // Number of distinct letters
        //Work Cited: https://www.tutorialspoint.com/javaregex/javaregex_pattern_matches.htm
        long numberOfDistinctLetters = inputFile
                .flatMap(content -> Arrays.asList(content.split("")))
                .filter(symbol -> Pattern.matches("[a-zA-Z]", symbol))
                .distinct()
                .count();
        //Print Out
        System.out.println("Task 5: Find the number of distinct letters in \"All's Well That Ends Well\".");
        System.out.println("Number of distinct letters: " + numberOfDistinctLetters);


        // Prompt the user to enter a word
        Scanner scanner = new Scanner(System.in);
        System.out.println("Task 6: Ask your user to enter a word and show all of the lines of \"All's Well That Ends Well\" that contain that word.");
        System.out.print("Enter a word: \n");
        String searchWord = scanner.nextLine();
        searchAndDisplayLines(inputFile, searchWord);

        sparkContext.stop();
    }
    private static void searchAndDisplayLines(JavaRDD<String> inputFile, String searchWord) {
        List<String> matchingLines = inputFile.filter(content -> content.contains(searchWord)).collect();
        for (String line : matchingLines) {
            System.out.println(line);
        }
    }
    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0]);
    }
}