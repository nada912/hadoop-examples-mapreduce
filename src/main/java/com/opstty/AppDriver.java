package com.opstty;

import com.opstty.job.*;

import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", Districts.class,
                    "A map/reduce program that returns the districts containing trees only.");
            programDriver.addClass("speciesCount", SpeciesCount.class,
                    "A map/reduce program that returns the tree species counts.");
            programDriver.addClass("maxHeight", MaxHeight.class,
                    "A map/reduce program that returns the maximum height for each species.");
            programDriver.addClass("sortedTrees", SortedTrees.class,
                    "A map/reduce program that returns trees sorted by height.");
            programDriver.addClass("sortedOldestTrees", SortedOldestTrees.class,
                    "A map/reduce program that returns the district with oldest tree.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
