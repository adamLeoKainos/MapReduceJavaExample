package io.datamass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words=line.split("\t");
        ArrayList<Integer> list = new ArrayList<Integer>();
        for(int i=0; i<words.length; i++)
        {
            String word = i+"-"+words[i];
            Text outputKey = new Text(word.toUpperCase().trim());
            IntWritable outputValue = new IntWritable(1);
            con.write(outputKey, outputValue);

            if (i>=list.size()) {
                list.add(1);
            } else {
                int temp = list.get(i);
                list.set(i,temp+1);
            }
        }

        for (int z=0; z<list.size(); z++) {
            Text outputKey = new Text(z+"-");
            IntWritable outputValue = new IntWritable(list.get(z));
            con.write(outputKey, outputValue);
        }
    }

}
