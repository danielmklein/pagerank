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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// TODO: move mapper and reducer to their own classes,
// add run() method to this class, following example of
// github pagerank (so this class should extend Tool or whatever.)
// so main() should just call run().
public class PageRankDriver extends Configured implements Tool {

    /*public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(PageRank.class);
      conf.setJobName("PageRank");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(LongWritable.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      conf.setMapperClass(Map.class);

      if (args.length != 2) {
          System.out.println("Usage:");
          System.out.println("/path/to/input/files /path/to/output");
          System.exit(1);
      }

      String graphDefPath = args[0];
      // TODO: open graphDefPath
      // read num nodes
      // initialize list of nodes
      // read num edges?
      // for adjacency in the rest of file:
      //    add adjacent node id to list for node
      // finally, write each nid, init pagerank, and adj list to input file.

      String inputPath = "/pagerank/init_input.data";
      FileInputFormat.setInputPaths(conf, new Path(inputPath));
      //FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      JobClient.runJob(conf);
    }*/
    private static NumberFormat nf = new DecimalFormat("00");


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(), args));
        // TODO: we could specify the exact input file as an arg...
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted;
        String lastResultPath = null;

        prepareInputFile();

        // TODO: num iterations should go in this loop
        for (int runs = 0; runs < 0; runs++) {
            String inPath = "pagerank/input/iter" + nf.format(runs);
            lastResultPath = "pagerank/input/iter" + nf.format(runs + 1);

            isCompleted = calculate(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        return 0;
    }

    private void prepareInputFile() throws Exception, IOException
    {
      Configuration config = new Configuration();
      config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
      config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
      try
      {
        // TODO: read /pagerank/graph.txt, construct hashmap of nid:node pairs,
        // then write it to /pagerank/input/iter00
        FileSystem fs = FileSystem.get(config);
        Path path = new Path("/pagerank/graph.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        Map<String, List<String>> nodes = new HashMap<String, List<String>>();
        String line;

        line = br.readLine();
        // pull out num nodes and num edges
        Integer numNodes = Integer.parseInt(line.split("\\s+")[0]);
        System.out.println("Number of nodes is: " + numNodes);
        Integer numEdges = Integer.parseInt(line.split("\\s+")[1]);
        System.out.println("Number of edges is: " + numEdges);

        line = br.readLine();
        // pull out num iterations to run
        Integer numIterations = Integer.parseInt(line.trim());
        System.out.println("Number of iterations is: " + numIterations);

        line = br.readLine();
        String fromNodeId;
        String toNodeId;
        while (line != null)
        {
          // pull out the outlink and add it to the proper node
          fromNodeId = line.split("\\s+")[0].trim();
          toNodeId = line.split("\\s+")[1].trim();

          if (nodes.get(fromNodeId) == null)
          { // if fromNode not in table already, add it and add the outlink
            System.out.println("Adding node " + fromNodeId + " to table.");
            nodes.put(fromNodeId, new ArrayList<String>());
            System.out.println("Adding link from " + fromNodeId + " to " + toNodeid + " to the table.");
            nodes.get(fromNodeId).add(toNodeId);
          } else
          { // otherwise, just add toNodeId to the outlinks of fromNodeId
            nodes.get(fromNodeId).add(toNodeId);
          }

          line = br.readLine();
        }
      }
      catch (Exception e)
      {
        //
      }

      // TODO: might need to return the num nodes, num edges, and num iterations.
    }

    private boolean calculate(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException
    {
      Configuration conf = new Configuration();

      Job pageRank = Job.getInstance(conf, "PageRank");
      pageRank.setJarByClass(PageRankDriver.class);

      pageRank.setInputFormatClass(TextInputFormat.class);
      pageRank.setOutputKeyClass(Text.class);
      pageRank.setOutputValueClass(LongWritable.class);
      pageRank.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.setInputPaths(pageRank, new Path(inputPath));
      FileOutputFormat.setOutputPath(pageRank, new Path(outputPath));

      pageRank.setMapperClass(PageRankMapper.class);
      pageRank.setReducerClass(PageRankReduce.class);

      return pageRank.waitForCompletion(true);
  }

}
