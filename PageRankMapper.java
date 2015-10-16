import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

  private Text word = new Text();

  @Override
  public void map(LongWritable key, Text value,
                  Context context) throws IOException, InterruptedException {
    String line = value.toString();
    List<String> fields = Arrays.asList(line.split("\\s+"));
    String curNid = fields.get(0);
    Float initPageRank = Float.parseFloat(fields.get(1));
    Float portion = initPageRank / (new Float(fields.size() - 2));

    System.out.println("MAPPER: node " + curNid + " has current value " + initPageRank);
    System.out.println("MAPPER: node " + curNid + " has neighbors: ");

    for (String outlinkId : fields.subList(2, fields.size() - 1))
    {
      System.out.println(outlinkId);
      word.set(outlinkId);
      //output.collect(word, new FloatWritable(portion));
      context.write(word, new FloatWritable(portion));
    }
    // parse nid, init_pagerank, and outlinks from line
    // for nid in outlinks:
    //    emit(nid, init_pagerank/len(outlinks))
  }
}
