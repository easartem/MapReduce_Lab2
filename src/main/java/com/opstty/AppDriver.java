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

            programDriver.addClass("districtWithTrees", Job1.class,
                    "A map/reduce program that counts the number of district with trees.");

            programDriver.addClass("speciesList", Job2.class,
                    "A map/reduce program that list the species of trees.");

            programDriver.addClass("nbTrees", Job3.class,
                    "A map/reduce program that counts the number of trees by species.");

            programDriver.addClass("maxHeight", Job4.class,
                    "A map/reduce program that calculates the maximum height for each species.");

            programDriver.addClass("treesSort", Job5.class,
                    "A map/reduce program that sort the trees heigt from smallest to largest.");

            programDriver.addClass("eldestTreeDistrict", Job6.class,
                    "A map/reduce program that find out the district with the eldest tree");

            programDriver.addClass("greenestDistrict", Job7.class,
                    "A map/reduce program that find out the district with the most trees");

            exitCode = programDriver.run(argv);

        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
