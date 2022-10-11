package com.wwl.docClassCount;

import com.wwl.fileMerge.SmallFilesToSequenceFileConverter;
import com.wwl.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/***
 *统计每个类对应的文件数量
 * <docName,content> ---> <docClass,1>
 */
public class DocClassCount extends Configured implements Tool {
    static class DocCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private  IntWritable one=new IntWritable(1);
        private  Text docClass=new Text();
        @Override
        protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] splits = key.toString().split("_");
            docClass.set(splits[0]);
            context.write(docClass, one);
        }
    }
    /***
     * map端先combine
     */
    static class DocCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable docCounts = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            docCounts.set(sum);
            System.out.println(key.toString());
            context.write(key, docCounts);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        // 如果输出目录存在，则先删除输出目录
       // Path outputPath = new Path(Utils.DOC_CLASS_COUNT_OUTPUT);
        Path outputPath = new Path(Utils.LOCAL_TEST_DOC_CLASS_COUNT_OUTPUT);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);
        Job job = Job.getInstance(conf, "DocClassCount");
        job.setJarByClass(DocClassCount.class);
        job.setMapperClass(DocCountMapper.class);
        job.setCombinerClass(DocCountReducer.class);
        job.setReducerClass(DocCountReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(Utils.LOCAL_TEST_OUTPUT));
        FileOutputFormat.setOutputPath(job, new Path(Utils.LOCAL_TEST_DOC_CLASS_COUNT_OUTPUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new DocClassCount(), args);
        System.out.println("计算每个类有多少文件");
        System.exit(exitCode);
    }
}
