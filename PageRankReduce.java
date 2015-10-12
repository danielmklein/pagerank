public static class PageRankReduce extends MapReduceBase
  implements Reducer<Text, LongWritable, Text, LongWritable> {

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
