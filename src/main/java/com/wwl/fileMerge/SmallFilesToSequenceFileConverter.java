package com.wwl.fileMerge;

import com.wwl.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/***
 * 多个小文件合并成flat file
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{
        private Text fileNameKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path=((FileSplit) split).getPath();
            Path classNamePath=path.getParent();
            fileNameKey=new Text(classNamePath.getName()+"_"+path.toString());// className_文件名作为key

        }
        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileNameKey, value);
        }

    }
    @Override
    public int run(String[] strings) throws Exception {
        for(String ss:strings)
            System.out.println(ss);
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        //获取输入路径
      //  Path inputPath = new Path(Utils.TRAIN_DATA_INPUT);
    //    Path outputPath = new Path(Utils.TRAIN_DATA_SEQUENCE_OUTPUT);
        //Path inputPath = new Path(Utils.LOCAL_TEST_INPUT);
       // Path outputPath = new Path(Utils.LOCAL_TEST_OUTPUT);
       Path inputPath = new Path("/home/wwl/hadoop/test");
       Path outputPath = new Path("/home/wwl/hadoop/testOutput");
        if (fs.exists(outputPath))
            fs.delete(outputPath, true);
        Job job = Job.getInstance(conf, "SmallFilesToSequenceFileConverter");
        job.setJarByClass(SmallFilesToSequenceFileConverter.class);//设置包含类的Jar包
        job.setMapperClass(SequenceFileMapper.class);
        job.setOutputKeyClass(Text.class);//文件名
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileStatus[] fileStatusList = fs.listStatus(inputPath);
        String[] tmpList = new String[fileStatusList.length];//获取所有训练文件的路径
        for (int i = 0; i < tmpList.length; ++i)
            tmpList[i] = fileStatusList[i].getPath().toString();
        for (String path : tmpList)
            WholeFileInputFormat.addInputPath(job, new Path(path));
        SequenceFileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SmallFilesToSequenceFileConverter(), args);
        System.out.println("小文件打包完毕");
        System.exit(exitCode);
    }
}
