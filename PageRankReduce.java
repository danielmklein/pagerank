import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReduce extends MapReduceBase
  implements Reducer<Text, LongWritable, Text, LongWritable> {

  @Override
  public void reduce(Text key, Iterator<LongWritable> values,
                     OutputCollector<Text, LongWritable> output,
                     Reporter reporter) throws IOException {
    long sum = 0;
    while (values.hasNext()) {
      sum += values.next().get();
    }
    output.collect(key, new LongWritable(sum));
  }
}
