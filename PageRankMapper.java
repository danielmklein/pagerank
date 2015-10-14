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

public static class PageRankMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, LongWritable> {

  private Text word = new Text();

  public void map(LongWritable key, Text value,
                  OutputCollector<Text, LongWritable> output,
                  Reporter reporter) throws IOException {
    String line = value.toString();
    List<String> fields = line.split();
    String curNid = fields[0];
    Long initPageRank = Long.parseLong(fields[1]);
    Long portion = initPageRank / (fields.size() - 2);

    for (String outlinkId : fields.subList(2, fields.size() - 1))
    {
      word.set(outlinkId);
      output.collect(word, new LongWritable(portion));
    }
    // parse nid, init_pagerank, and outlinks from line
    // for nid in outlinks:
    //    emit(nid, init_pagerank/len(outlinks))

    StringTokenizer itr = new StringTokenizer(line);
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      output.collect(word, one);
    }
  }
}
