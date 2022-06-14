package com.wordcount;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BCESalaryReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    private PriorityQueue<BCERecord> queue;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        queue = new PriorityQueue<>();
    }

    @Override
    protected void reduce(LongWritable positionKey, Iterable<IntWritable> salariesValues, Context context)
            throws IOException, InterruptedException {
        Long position = positionKey.get();
        Integer salary = salariesValues.iterator().next().get();
        queue.add(new BCERecord(position, salary));
        if (queue.size() > 10) {
            queue.poll();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        while (!queue.isEmpty()) {
            BCERecord[] result = new BCERecord[10];
            for (int i = 9; i >= 0; i--) {
                result[i] = queue.poll();
            }
            for (BCERecord record : result) {
                LongWritable key = new LongWritable(record.position);
                IntWritable value = new IntWritable(record.salary);
                context.write(key, value);
            }
        }
    }
}
