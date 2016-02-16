package hu.sztaki.mbalassi.flink.ials.tester.utils

object SparkUtils {
    def deleteDir(dirPath : String) : Unit = {
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(dirPath), hadoopConf)
      try { hdfs.delete(new org.apache.hadoop.fs.Path(dirPath), true) } catch { case _ : Throwable => { } }

    }
}
