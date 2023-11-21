package org.data;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.feature.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws IOException {

        String data = "10k.github.jsonl";

        SparkSession spark = SparkSession
                .builder()
                .appName("DataEngineering")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().json(data);

        Dataset<Row> filtered = df.filter("type == 'PushEvent'")
                .select(
                        df.col("actor.login").alias("Author"),
                        df.col("payload.commits.message").alias("Commit")
                );

        Dataset<Row> filteredToString = filtered
                .withColumn("Author", concat_ws(" ", filtered.col("Author")))
                .withColumn("Commit", concat_ws(" ", filtered.col("Commit")))
                .filter("Commit != ''");

        Dataset<Row> regexDf = filteredToString.withColumn("Commit",
                        functions.regexp_replace(filteredToString.col("Commit"),
                                "[^0-9a-zA-Z]+", " "));

        List<String> authors = new ArrayList<>();
        List<String> commits = new ArrayList<>();

        regexDf.select(regexDf.col("Author"), regexDf.col("Commit"))
                .collectAsList()
                .stream()
                .forEach(row -> {
                    authors.add(row.getString(0));
                    commits.add(row.getString(1));
                });

            File csv = new File("3gram.csv");
            csv.createNewFile();

            FileWriter writer = new FileWriter(csv);

            for (int i = 0; i <= authors.size()-1; i++) {

                List<String> ngrams = generateNgrams(commits.get(i), 3);
                String collect = authors.get(i) + "," + ngrams.stream().collect(Collectors.joining(",")) + "\n";

                if (ngrams.size() == 3) { // optional, can remove
                    writer.write(collect);
                }

                /*if (!ngrams.isEmpty()) { // uncomment if necessary
                    writer.write(collect);
                }*/
            }

    }

    private static List<String> generateNgrams(String wordStr, int n) {

        List<String> word = Arrays.asList(wordStr.toLowerCase().split(" "));

        List<String> ngrams = new ArrayList<>();
        for(int i = 0; i <= word.size(); i++){
            if ( i < word.size() - n + 1) {
                String ngram = "";

                for (int j = 0; j < n-1; j++) {
                    ngram += word.get(i + j) + "-";
                }

                ngram += word.get(i + n - 1);
                ngrams.add(ngram.trim());

                if (ngrams.size() >= 3){ // optional, can remove
                    break;
                }

            }
        }
        return ngrams;
    }
}