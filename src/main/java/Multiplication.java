import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieA:movieB \t relation

			//pass data to reducer
			//key: movieB  value: movieA:relation

			String[] movieA_movieBrelation = value.toString().trim().split("\t");
			String[] movieA_movieB = movieA_movieBrelation[0].trim().split(":");
			String relation = movieA_movieBrelation[1].trim();

			String movieA = movieA_movieB[0];
			String movieB = movieA_movieB[1];

			context.write(new Text(movieB), new Text(movieA + ":" + relation));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer
			String[] user_movie_rating = value.toString().trim().split(",");
			if (user_movie_rating.length<3){
				return;
			}
			String user = user_movie_rating[0];
			String movie = user_movie_rating[1];
			String rating = user_movie_rating[2];

			context.write(new Text(movie), new Text(user + "=" + rating));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication

			Map<String, Double> coOccurence_map = new HashMap<String, Double>();
			Map<String, Double> rating_map = new HashMap<String, Double>();

			for (Text value: values){
				String tmp = value.toString();
				if (tmp.contains("=")){
					String[] user_rating = tmp.split("=");
					String user = user_rating[0].trim();
					String rating = user_rating[1].trim();
					rating_map.put(user,Double.valueOf(rating));
				}else{
					String[] user_rating = tmp.split(":");
					String movieA = user_rating[0].trim();
					String relation = user_rating[1].trim();
					coOccurence_map.put(movieA,Double.valueOf(relation));
				}
			}

			for (Map.Entry<String, Double> coOccurence_entry : coOccurence_map.entrySet()){
				String movieA = coOccurence_entry.getKey();
				Double relation = coOccurence_entry.getValue();

				for (Map.Entry<String, Double> rating_entry : rating_map.entrySet()){
					String user = rating_entry.getKey();
					Double rating = rating_entry.getValue();

					Double sub_expectation = relation * rating;

					context.write(new Text(movieA + ":" + user), new DoubleWritable(sub_expectation));
				}
			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
