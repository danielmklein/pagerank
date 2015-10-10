/*
/  TODO:
/   -- write driver
      -- see https://github.com/abij/hadoop-wiki-pageranking/blob/master/src/com/xebia/sandbox/hadoop/WikiPageRanking.java
      -- driver needs to parse graph.txt and construct initial input file into mapreduce
      -- each intermediate result should be written to its own file
    -- write mapper
      -- see https://github.com/abij/hadoop-wiki-pageranking/blob/master/src/com/xebia/sandbox/hadoop/job2/calculate/RankCalculateMapper.java

    -- write reducer
      -- see https://github.com/abij/hadoop-wiki-pageranking/blob/master/src/com/xebia/sandbox/hadoop/job2/calculate/RankCalculateReduce.java
**/

/*
/ mapper gets node_id, cur_pagerank_val, and outlink_list.
/ for each nid in outlink_list:
/    emit(nid, cur_pagerank_val/len(outlink_list))
/
////////////////////////////////////////////////////////
/
/ each reducer gets list of pagerank_vals for given nid
/ emit(nid, sum(pagerank_vals))
/
////////////////////////////////////////////////////////
/
/ do i really need to emit the graph structure?
/ can't the driver take care of that?
/
////////////////////////////////////////////////////////
/
/ so, my driver needs to initially build an input file
/ containing lines of data such as:
/ "nid init_pagerank outlink1 outlink2 outlink3 ... outlinkn"
////////////////////////////////////////////////////////
/
/ building and running:
/  $ mkdir Gender_classes
/ $ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d PageRank_classes PageRank.java
/ $ jar -cvf /home/hadoop/PageRank.jar -C PageRank_classes/ .
/
/ $ hadoop PageRank.jar PageRank input_path output_path
**/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank {

    public static class MapClass extends MapReduceBase
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

    public static class Reduce extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, IntWritable> {

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

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(PageRank.class);
      conf.setJobName("PageRank");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(LongWritable.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      conf.setMapperClass(Map.class);

      if (args.length != 2) {
          System.out.println("Usage:");
          System.out.println(" /path/to/input/files /path/to/output");
          System.exit(1);
      }

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      JobClient.runJob(conf);
    }
}
