package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/** La classe StringTokenizer permet de décomposer une chaîne de caractères en une suite de "mots" séparés par des "délimiteurs".
 *  La classe StringTokenizer fournit deux méthodes importantes qui permettent d'obtenir les différentes parties d'une chaîne l'une
 *  après l'autre : la méthode hasMoreTokens indique s'il reste des éléments à extraire; la méthode nextToken renvoie l'élément suivant.
 */

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
