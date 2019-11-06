package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataStream
{
	private static double delta = Math.exp(- 5.0);
	private static double epsilon = Math.E * Math.pow(10.0, - 4.0);
	private static int prime = 123457;
	private static int buckets = (int) (Math.E / epsilon);

	private static List<Integer> dataStreamHashFunction(List<Tuple2<Integer, Integer>> ab, int prime, int buckets, int x)
	{
		List<Integer> hashedValues = new ArrayList<>();
		for (int i = 0; i < ab.size(); i++)
		{
			int y = x % prime;
			int v = (ab.get(i)._1 * y + ab.get(i)._2) % prime;
			int r = v % buckets;
			hashedValues.add(r);
		}
		return hashedValues;
	}

	public static void main(String[] args) throws Exception
	{
		SparkConf conf = new SparkConf().setAppName("DataStream").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> params_file = sc.textFile("hash_params.txt");
		JavaRDD<Tuple2<Integer, Integer>> params = params_file.map(line -> Arrays.asList(line.split("[^\\w]")))
				.map(line -> new Tuple2<>(Integer.parseInt(line.get(0)), Integer.parseInt(line.get(1))));
		List<Tuple2<Integer, Integer>> paramsList = params.collect();

		double[][] matrix = new double[paramsList.size()][buckets];
		int record_num = 0;

		String dataStreamFileName = "data_stream.txt";
		File dataStreamInputFile = new File(dataStreamFileName);
		FileInputStream dataStreamInputFileStream = new FileInputStream(dataStreamInputFile);
		InputStreamReader dsISR = new InputStreamReader(dataStreamInputFileStream);
		BufferedReader dsBR = new BufferedReader(dsISR);
		String line;

		while ((line = dsBR.readLine()) != null)
		{
			record_num++;
/*			if (record_num % (Math.pow(10.0, 6.0)) == 0)
			{
				System.out.println(record_num + "records are processed ...");
			}*/
			int x = Integer.parseInt(line);
			List<Integer> hashVal = dataStreamHashFunction(paramsList, prime, buckets, x);

			for (int i = 0; i < paramsList.size(); i++)
			{
				matrix[i][hashVal.get(i)] += 1;
			}
		}
		dsBR.close();
		//System.out.println(record_num + "records");

		List<Double> error_Record = new ArrayList<>();
		List<Double> freq_Record = new ArrayList<>();

		//calculate the error
		String countsFileName = "counts.txt";
		File countsInputFile = new File(countsFileName);
		FileInputStream countsInputFileStream = new FileInputStream(countsInputFile);
		InputStreamReader countsISR = new InputStreamReader(countsInputFileStream);
		BufferedReader countsBR = new BufferedReader(countsISR);
		String lines;
		int count_num = 0;
		while ((lines = countsBR.readLine()) != null)
		{
			count_num++;
	/*		if(count_num % (Math.pow(10.0,4.0)) == 0){
				System.out.println(count_num + "counts are processed ...");
			}*/
			String[] itemsFromFile = lines.split("[^\\w]");
			int idx = Integer.parseInt(itemsFromFile[0]);
			int fi = Integer.parseInt(itemsFromFile[1]);
			List<Integer> hashVal = dataStreamHashFunction(paramsList, prime, buckets, idx);
			double fiMax = Double.MAX_VALUE;

			for (int i = 0; i < paramsList.size() ; i++)
			{
				fiMax = Math.min(fiMax, matrix[i][hashVal.get(i)]);
			}
			double error = (fiMax - fi) / (fi * 1.0);
			error_Record.add(error);
			freq_Record.add(fi / (1.0 * record_num));
		}
		countsBR.close();
		//System.out.println(count_num + "words");
		JavaRDD<Double> error_RecordRDD = sc.parallelize(error_Record);
		JavaRDD<Double> feq_RecordRDD = sc.parallelize(freq_Record);
		error_RecordRDD.saveAsTextFile("error_recordss.txt");
		feq_RecordRDD.saveAsTextFile("feq_recordss.txt");
		sc.stop();
	}
}
