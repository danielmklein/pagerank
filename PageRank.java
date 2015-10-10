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
**/
