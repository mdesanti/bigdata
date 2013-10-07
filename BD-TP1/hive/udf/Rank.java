package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class RankYear extends UDF{
    private int  counter;
    private Integer last_key;
    public int evaluate(final Integer key){
      if ( !key.equals(this.last_key) ) {
         this.counter = 0;
         this.last_key = key;
      }
      return this.counter++;
    }
}