package com.wwl.wordCount;

import com.wwl.docClassCount.DocClassCount;
import com.wwl.textPair.TextPair;
import com.wwl.util.Utils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/***
 * 计算每个文件的单词数
 * 供条件概率使用
 */
public class WordCount extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCount.class);
    static class WordCountMapper extends Mapper<Text, BytesWritable, TextPair, IntWritable> {
        private final Pattern WORDPATERN=Pattern.compile("^[A-Za-z]{2,}$");
        private Text word=new Text();
        private  IntWritable one=new IntWritable(1);
        @Override
        protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, TextPair, IntWritable>.Context context) throws IOException, InterruptedException {
            String docClassName=key.toString().split("_")[0];
            String content = new String(value.getBytes(),0, value.getLength());// 获取单词串
         //   System.out.println(content);
            String[] wordList = content.split("\\s+");//切割为单词
            for (String word : wordList) {
//                if(WORDPATERN.matcher(word).find()) {
//                    TextPair tp = new TextPair(docClassName, word);
//                    context.write(tp,one);
//                }
//                else
//                    log.debug("过滤无用词：" + word);
               System.out.println(word);
                TextPair tp = new TextPair(docClassName, word);
                context.write(tp,one);
            }
        }
    }
    static class WordCountReducer extends Reducer<TextPair, IntWritable, Text, IntWritable>{
        private  IntWritable wordCount=new IntWritable();
        private  Text wordFromClassCount=new Text();
        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Reducer<TextPair, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value:values)
                sum+=value.get();
            wordCount.set(sum);
            wordFromClassCount.set(key.toString());
            context.write(wordFromClassCount,wordCount);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        // 如果输出目录存在，则先删除输出目录
        // Path outputPath = new Path(Utils.DOC_CLASS_COUNT_OUTPUT);
        Path outputPath = new Path(Utils.LOCAL_TEST_WORD_CLASS_COUNT_OUTPUT);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
     //   job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(TextPair.class);//k2
        job.setMapOutputValueClass(IntWritable.class);//v2
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

       // FileInputFormat.addInputPath(job, new Path(Utils.LOCAL_TEST_OUTPUT));//
        FileInputFormat.addInputPath(job, new Path("/home/wwl/hadoop/testOutput"));//
        FileOutputFormat.setOutputPath(job, new Path(Utils.LOCAL_TEST_WORD_CLASS_COUNT_OUTPUT));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.out.println("统计每个类中的单词数");
        System.exit(exitCode);
    }
}
