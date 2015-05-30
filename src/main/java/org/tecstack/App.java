package org.tecstack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args )
	{
		try {
			Configuration conf=new Configuration();
			FileSystem hdfs=FileSystem.get(conf);
			Path dst =new Path("/user/promise/");
			FileStatus files[]=hdfs.listStatus(dst);
			for(FileStatus file:files) {
				System.out.println(file.getPath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
