package com.wwl;

import com.wwl.fileMerge.SmallFilesToSequenceFileConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        SmallFilesToSequenceFileConverter smallFilesPackage = new SmallFilesToSequenceFileConverter();
        ToolRunner.run(conf, smallFilesPackage, args);
    }
}