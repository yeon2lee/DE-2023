import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200994 {

    public static class IMDB {
        public String movie;
        public double rating;

        public IMDB(String movie, double rating) {
            this.movie = movie;
            this.rating = rating;
        }

        public String getMovie() {
            return this.movie;
        }

        public Double getRating() {
            return this.rating;
        }

        public String getString() {
            return movie + " " + rating;
        }
    }

    public static class DoubleString implements WritableComparable {
        String joinKey = new String();
        String tableName = new String();

        public DoubleString() {}
        public DoubleString( String _joinKey, String _tableName ) {
            joinKey = _joinKey;
            tableName = _tableName;
        }

        public void readFields(DataInput in) throws IOException {
            joinKey = in.readUTF();
            tableName = in.readUTF();
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(joinKey);
            out.writeUTF(tableName);
        }

        public int compareTo(Object o1) {
            DoubleString o = (DoubleString) o1;
            int ret = joinKey.compareTo( o.joinKey );
            if (ret != 0) return ret;
            return tableName.compareTo( o.tableName);
        }

        public String toString() { return joinKey + " " + tableName; }
    }


    public static class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
            super(DoubleString.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleString k1 = (DoubleString)w1;
            DoubleString k2 = (DoubleString)w2;
            int result = k1.joinKey.compareTo(k2.joinKey);
            if (0 == result) {
                result = k1.tableName.compareTo(k2.tableName);
            }
            return result;
        }

    }

    public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
        public int getPartition(DoubleString key, Text value, int numPartition) {
            return key.joinKey.hashCode()%numPartition;
        }
    }

    public static class FirstGroupingComparator extends WritableComparator {
        protected FirstGroupingComparator() {
            super(DoubleString.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleString k1 = (DoubleString)w1;
            DoubleString k2 = (DoubleString)w2;
            return k1.joinKey.compareTo(k2.joinKey);
        }
    }

    public static class IMDBComparator implements Comparator<IMDB> {
        @Override
        public int compare(IMDB o1, IMDB o2) {
            if (o1.rating > o2.rating) {
                return 1;
            } else if (o1.rating == o2.rating) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    public static void insertIMDB(PriorityQueue q, String movie, double rating, int topK) {
        IMDB imdb_head = (IMDB) q.peek();
        if (q.size() < topK || imdb_head.rating < rating) {
            IMDB imdb = new IMDB(movie, rating);
            q.add(imdb);
            if(q.size() > topK) q.remove();
        }
    }

    public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text> {
        boolean movieFile = true;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "::");
            DoubleString outputKey = new DoubleString();
            Text outputValue = new Text();

            if (movieFile) {
                String id = itr.nextToken();
                String title = itr.nextToken();
                String genre = itr.nextToken();

                itr = new StringTokenizer(genre, "|");
                boolean isFantasy = false;
                while (itr.hasMoreTokens()) {
                    if (itr.nextToken().equals("Fantasy")) {
                        isFantasy = true;
                        break;
                    }
                }

                if (isFantasy) {
                    outputKey = new DoubleString(id, "Movies");
                    outputValue.set("Movies," + title);
                    context.write( outputKey, outputValue );
                }

            } else {
                String id = itr.nextToken();
                String rating = itr.nextToken();

                outputKey = new DoubleString(id, "Ratings");
                outputValue.set("Ratings," + rating);
                context.write( outputKey, outputValue );
            }

        }

        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
            else movieFile = false;
        }
    }

    public static class IMDBReducer extends Reducer<DoubleString, Text, Text, Text> {
        private PriorityQueue<IMDB> queue;
        private Comparator<IMDB> comp = new IMDBComparator();
        private int topK;
        public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            String movieTitle = "";
            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                String file_type = itr.nextToken();

                if (count == 0) {
                    if (!file_type.equals("Movies")) {
                        break;
                    }
                    movieTitle = itr.nextToken();
                } else {
                    sum += Double.parseDouble(itr.nextToken());
                }

                count++;
            }

            if (sum != 0) {
                double average = sum / count;
                insertIMDB(queue, movieTitle, average, topK);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: IMDBStudent20200994 <in> <out>");
            System.exit(2);
        }

        conf.setInt("topK", Integer.valueOf(otherArgs[2]));
        Job job = new Job(conf, "IMDBStudent20200994");
        job.setJarByClass(IMDBStudent20200994.class);
        job.setMapperClass(IMDBMapper.class);
        job.setReducerClass(IMDBReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(DoubleString.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
