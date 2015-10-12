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
