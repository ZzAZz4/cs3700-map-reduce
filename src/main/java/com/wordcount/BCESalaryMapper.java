package com.wordcount;

import com.opencsv.CSVReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BCESalaryMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    private PriorityQueue<BCERecord> queue;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        queue = new PriorityQueue<>();
    }

    @Override
    protected void map(LongWritable posKey, Text recordTextValue, Context context)
            throws IOException, InterruptedException {
        Optional<Integer> optSalary = parseSalary(recordTextValue.toString());
        if (!optSalary.isPresent()) {
            return;
        }
        BCERecord record = new BCERecord(posKey.get(), optSalary.get());
        queue.add(record);
        if (queue.size() > 10) {
            queue.poll();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        while (!queue.isEmpty()) {
            BCERecord record = queue.poll();
            LongWritable key = new LongWritable(record.position);
            IntWritable value = new IntWritable(record.salary);
            context.write(key, value);
        }
    }

    private static Optional<Integer> parseSalary(String record)
            throws IOException {
        try (CSVReader reader = new CSVReader(new StringReader(record))) {
            String[] entries = reader.readNext();
            Integer salary = Integer.parseInt(entries[6]);
            return Optional.of(salary);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
