package ds;
//Work Cited: https://github.com/CMU-Heinz-95702/lab9-MapReduceAndSpark
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.regex.Pattern;
import java.util.Arrays;

/**
 * @goal      Perform word count and symbol analysis on a text file using Apache Spark.
 * @return    void
 * @author   Xinyuan Yang(xy3)
 * @email    xy3@andrew.cmu.edu
 */

public class WordCounter {

    private static void wordCount(String fileName) {


            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

            JavaRDD<String> inputFile = sparkContext.textFile(fileName);

            // Number of lines
            long numberOfLines = inputFile.count();


            // Split the line into an array of words
            JavaRDD<String> WordCount = inputFile.flatMap(line -> Arrays.asList(line.split("[^a-zA-Z]+")));
            // Deal with the blank
            // Work Cited: https://spark.apache.org/docs/1.0.2/api/java/org/apache/spark/api/java/JavaRDD.html
            JavaRDD<String> KnockoutBlank = WordCount.filter(word -> !word.isEmpty());

            // Number of words
            long numberOfWords = KnockoutBlank.count();


            // Number of distinct words
            long numberOfDistinctWords = KnockoutBlank.distinct().count();

            // Number of symbols
            long numberOfSymbols = inputFile.flatMap(content -> Arrays.asList(content.split(""))).count();

            // Number of distinct symbols
            long numberOfDistinctSymbols = inputFile.flatMap(content -> Arrays.asList(content.split("")))
                    .distinct().
                    count();


            // Number of distinct letters
            //Work Cited: https://www.tutorialspoint.com/javaregex/javaregex_pattern_matches.htm
            long numberOfDistinctLetters = inputFile
                    .flatMap(content -> Arrays.asList(content.split("")))
                    .filter(symbol -> Pattern.matches("[a-zA-Z]", symbol))
                    .distinct()
                    .count();


            //Print Out
            System.out.println("Number of lines: " + numberOfLines);
            System.out.println("Number of words: " + numberOfWords);
            System.out.println("Number of distinct words: " + numberOfDistinctWords);
            System.out.println("Number of symbols: " + numberOfSymbols);
            System.out.println("Number of distinct symbols: " + numberOfDistinctSymbols);
            System.out.println("Number of distinct letters: " + numberOfDistinctLetters);

        sparkContext.stop();
        }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0]);
    }
}