import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

  @Override
  public void reduce(Text key, Iterable<FloatWritable> values,
                     Context context) throws IOException, InterruptedException {
    System.out.println("REDUCER: found following portions for node " + key);
    Float sum = new Float(0);
    for (FloatWritable value : values)
    {
      System.out.println(value);
      sum += value.get();
    }
    System.out.println("REDUCER: final value for " + key + " is " + sum);
    context.write(key, new FloatWritable(sum));
  }
}
