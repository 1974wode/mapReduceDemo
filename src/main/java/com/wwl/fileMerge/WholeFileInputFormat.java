package com.wwl.fileMerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 小文件合并成sequenceFile,自定义输入输出格式
 * 输入key为空，value为整个文件内容
 * 输出为文件名 对应内容
 */
public class WholeFileInputFormat  extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;           //保存输入的分片，它将被转换成一条（key，value）记录
    private Configuration conf;     //配置对象
    private Text key = new Text();        //key对象，初始值为空
    private BytesWritable value = new BytesWritable(); //value对象，内容为空
    private boolean processed  = false;   //布尔变量记录记录是否被处理过
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;  	     //将输入分片强制转换成FileSplit
        this.conf = taskAttemptContext.getConfiguration();  //从context获取配置信息
    }
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs=file.getFileSystem(conf);
            FSDataInputStream in=null;//定义文件输入对象
            try {
                in=fs.open(file);
                IOUtils.readFully(in,contents,0,contents.length);
                value.set(contents, 0, contents.length);
            }finally {
                IOUtils.closeStream(in);
            }
            processed=true;
            return  true;
        }
        return false;
    }
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }
    @Override
    public void close() throws IOException {
        //do nothing
    }
}
