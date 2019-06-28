import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Elem implements Writable {

    public int tag;
    public int index;
    public double value;
    Elem () {}

    Elem (int a,int b, double c ) {
        tag=a; 
        index = b;
        value = c;
   }

    public void write (DataOutput out) throws IOException {
        out.writeInt(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields (DataInput in) throws IOException {
        tag= in.readInt();
        index = in.readInt();
        value = in.readDouble();

    }
    
    public String toString () { return Double.toString(value); }
	 
 }


class Pair implements WritableComparable<Pair>{
	
    public int i;
    public int j;

    Pair (int a,int b) {
       i = a;  j = b;
    }

    Pair(){}

   public void write (DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

   public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }
   
   public String toString (){ return i+" "+j; }
	 
   public int compareTo(Pair p)

    {

        if (i==p.i)

        {
           if (j == p.j)
                return 0;
            else if(j < p.j)
                return -1;
            else
                return 1;
        }

        else if(i<p.i)
           return -1;
        else 
            return 1;
    }

}

class Result implements Writable {

    public double value;

    Result (Double val) {

    	value = val;

    }

    Result(){}

    public void write ( DataOutput out ) throws IOException {

        out.writeDouble(value);

    }

    public void readFields ( DataInput in ) throws IOException {

    	value = in.readDouble();

    }

    public String toString () { return Double.toString(value); }
	 
}



public class Multiply {
	   public static void main ( String[] args ) throws Exception {

	        Job job1 = Job.getInstance();

	        job1.setJobName("Matrix Multiplication-1");
	        job1.setJarByClass(Multiply.class);
	        job1.setMapOutputKeyClass(IntWritable.class);
	        job1.setMapOutputValueClass(Elem.class);
            MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,ElemM.class);
	        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,ElemN.class);
	        job1.setReducerClass(Reducer1.class);        
	        job1.setOutputKeyClass(Pair.class);
	        job1.setOutputValueClass(Result.class);       
	        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
	        job1.waitForCompletion(true);

	        

	        //setting the job 2

	        Job job2 = Job.getInstance();
	        
	        job2.setJobName("Matrix Multiplication-2");
            job2.setJarByClass(Multiply.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setMapperClass(Mapper2.class);
            job2.setMapOutputKeyClass(Pair.class);
	        job2.setMapOutputValueClass(Result.class);
            job2.setReducerClass(Reducer2.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2,new Path(args[3]));
            job2.waitForCompletion(true);

	    }


    public static class ElemM extends Mapper<Object,Text,IntWritable,Elem > {

        @Override
        public void map ( Object key, Text value, Context context )

                        throws IOException, InterruptedException {

            Scanner s = new Scanner(value.toString()).useDelimiter(",");

            int mRow = s.nextInt();
            int mCol = s.nextInt();
            double mVal = s.nextDouble();
            Elem mElem = new Elem(0,mRow,mVal);
            String keys = Integer.toString(mCol);
            context.write(new IntWritable(mCol),mElem);
            s.close();
        }

    }


    public static class ElemN extends Mapper<Object,Text,IntWritable,Elem> {

        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {

            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int nRow = s.nextInt();
            int nCol = s.nextInt();
            double nVal = s.nextDouble();
            Elem nElem = new Elem(1,nCol,nVal);
            String keys = Integer.toString(nRow);
            context.write(new IntWritable(nRow),nElem);
            s.close();
        }

    }


    public static class Reducer1 extends Reducer<IntWritable,Elem,Pair,Result> {

        static Vector<Elem> Mmatrix = new Vector<Elem>();
        static Vector<Elem> Nmatrix = new Vector<Elem>();

        @Override

        public void reduce ( IntWritable key, Iterable<Elem> values, Context context ) throws IOException, InterruptedException {

        	Mmatrix.clear();
        	Nmatrix.clear();

            for (Elem v: values)
               if (v.tag == 0){
                   Elem m = new Elem(0,v.index,v.value);
                   Mmatrix.add(m);
               }
            else {
                    Elem n = new Elem(1,v.index,v.value);
                    Nmatrix.add(n);
               }

            for (Elem m:Mmatrix)
        {

         for( Elem n : Nmatrix)

        {    Pair p=new Pair(m.index,n.index);   
        	 Double a = m.value*n.value;
        	 Result result=new Result(a);
            context.write(p,result);         
            }

    }                

        }

    }

    

    public static class Mapper2 extends Mapper<Object,Text,Pair,Result > {

        @Override

        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
           StringTokenizer s = new StringTokenizer(value.toString());
           while (s.hasMoreTokens()) {
                int nRow = Integer.parseInt(s.nextToken());
                int nCol = Integer.parseInt(s.nextToken());
                double prodValue = Double.parseDouble(s.nextToken());
                Pair mn = new Pair(nRow,nCol);
                Result res = new Result(prodValue);
                 context.write(mn,res);

            }

       }

    }


    public static class Reducer2 extends Reducer<Pair,Result,Text,DoubleWritable> {
     public void reduce ( Pair key, Iterable<Result> values, Context context ) throws IOException, InterruptedException {

           double result= 0.0;
           for (Result v: values){

                    result= result + v.value;

            }

            String resultKey= Integer.toString(key.i)+" "+Integer.toString(key.j);
            context.write(new Text(resultKey),new DoubleWritable(result));

        }

    }
  
}
