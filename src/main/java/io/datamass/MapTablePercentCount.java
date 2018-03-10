package io.datamass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapTablePercentCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words=line.split("\t");
        HashMap<Integer, ArrayList<Integer>> mapTotal = new HashMap<Integer, ArrayList<Integer>>();
        for(int i=0; i<words.length; i++)
        {
            String word = i+"-"+words[i];
            Text outputKey = new Text(word.toUpperCase().trim());
            IntWritable outputValue = new IntWritable(1);
            con.write(outputKey, outputValue);
            if (mapTotal.get(i)==null) {
                mapTotal.put(i, new ArrayList<Integer>());
                mapTotal.get(i).add(1);
            } else {
                mapTotal.get(i).add(1);
            }
        }


        Iterator it = mapTotal.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ArrayList<Integer>> pair = (Map.Entry)it.next();
            Text outputKey = new Text(pair.getKey().toString()+"-");
            IntWritable outputValue = new IntWritable(pair.getValue().size());
            con.write(outputKey, outputValue);
        }
    }

}
