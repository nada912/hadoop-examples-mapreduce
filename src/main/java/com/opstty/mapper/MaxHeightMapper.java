package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text word = new Text();
    private IntWritable tall = new IntWritable();

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        String[] trees = (value.toString()).split(";");
        if(!trees[0].equals("GEOPOINT")){ 
            String kind = trees[2]; 

            try{
                int taille = (int) Double.parseDouble(trees[6]);
                tall.set(taille);
                word.set(kind);
                context.write(word, tall);
            }catch (NumberFormatException e){}


        }
    }

}