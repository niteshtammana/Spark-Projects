package sparkstreaming

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

object twitterStreaming {

  def main(args: Array[String]): Unit = {

    //val ssc = new StreamingContext(args(0), "Twittercount", Seconds(10))
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwitterHashtag") // Loading configuration details
    val ssc = new StreamingContext(conf, Seconds(2)) // creating streaming context
    val hashTag = args(0) // first argument
    val outputDir = args(1) // second argument
    val cb = new ConfigurationBuilder() // loading configuration
    val prop = new Properties()
    val properties = new Properties() // loading properties
    // properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("twitter.properties"))
    properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("twitter.properties")) // loading all twitter properties
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(properties.getProperty("auth_consumer_key"))
      .setOAuthConsumerSecret(properties.getProperty("auth_consumer_secret"))
      .setOAuthAccessToken(properties.getProperty("auth_access_token"))
      .setOAuthAccessTokenSecret(properties.getProperty("auth_access_token_secret"))

    // getting the twitter keys
    val tf = new TwitterFactory(cb.build()) // establish connection with twitter
    val twitter = tf.getInstance() // create twitter object
    val auth = twitter.getAuthorization // get authorization
    //print(tf.getInstance())
    val filters = Array(hashTag) // Filter by keyword
    val twitterStream = TwitterUtils.createStream(ssc, Some(auth), filters) // create twitter dstreams filter by keyword


    val lines = twitterStream.map(status => status.getText) // get filtered output
    lines.print()
    val words = lines.flatMap(_.split(" ")) // split by empty space
    val pairs = words.map(word => (word, 1)) // split data
    val wordCounts = pairs.reduceByKey(_ + _) // perform wc
    wordCounts.print()


    lines.saveAsTextFiles(outputDir + "/" + "tweets") // save data


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
