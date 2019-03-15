package com.liuser.statistics
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Movie数据集，数据集字段通过分割
  *
  * 151^                          电影的ID
  * Rob Roy (1995)^               电影的名称
  * In the highlands ....^        电影的描述
  * 139 minutes^                  电影的时长
  * August 26, 1997^              电影的发行日期
  * 1995^                         电影的拍摄日期
  * English ^                     电影的语言
  * Action|Drama|Romance|War ^    电影的类型
  * Liam Neeson|Jessica Lange...  电影的演员
  * Michael Caton-Jones           电影的导演
  *
  * tag1|tag2|tag3|....           电影的Tag
  **/
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                  val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

/**
  * Rating数据集，用户对于电影的评分数据集，用，分割
  *
  * 1,           用户的ID
  * 31,          电影的ID
  * 2.5,         用户对于电影的评分
  * 1260759144   用户对于电影评分的时间
  */
case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
  * MongoDB的连接配置
  * @param uri   MongoDB的连接
  * @param db    MongoDB要操作数据库
  */
case class MongoConfig(val uri:String, val db:String)

/**
  * 推荐对象
  * @param rid   推荐的Movie的mid
  * @param r     Movie的评分
  */
case class Recommendation(rid:Int, r:Double)

/**
  * 电影类别的推荐
  * @param genres   电影的类别
  * @param recs     top10的电影的集合
  */
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    //统计的表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop100:27017/recommender",
            "mongo.db" -> "recommender"
        )

        val sparkConf: SparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //加入隐式转换
        import spark.implicits._

        //把数据加载进来
        val ratingDF: DataFrame = spark.read.option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql")
            .load().as[Rating].toDF()


        val movieDF: DataFrame = spark.read.option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION).format("com.mongodb.spark.sql")
            .load().as[Movie].toDF()

        //创建一张表
        ratingDF.createOrReplaceTempView("ratings")


        //统计所有历史数据中的每个电影的评分数
        // 数据结构化 ----> mid,count
        val rateMoreMoviseDF: DataFrame = spark.sql("select mid, count(mid) as count from ratings group by mid")

        rateMoreMoviseDF.write.option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql").save()


        //统计以月为单位拟每个电影的评分数
        //数据结构 -------> mid,count,time

        //时间格式化
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")

        //注册一个UDF函数，用于将timestamp装换成年月格式   1260759144000  => 201605
        spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)

        //将原来的Rating数据集中的时间转化为年月格式
        val ratingOfYeanMouth: DataFrame = spark.sql("select mid,score,changeDate(timestamp) as yeahmouth from ratings")

        //将新的数据集注册成为一张表
        ratingOfYeanMouth.createOrReplaceTempView("ratingOfMouth")
        val rateMOreRecentlyMovies: DataFrame = spark.sql("select mid, count(mid) as count ,yeahmouth from ratingOfMouth group by yeahmouth,mid")

        rateMOreRecentlyMovies.write
            .option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_RECENTLY_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

//        统计电影的平均分
        val averageMoviesDF: DataFrame = spark.sql("select mid , avg(score) as avg from ratings group by mid")


        averageMoviesDF.write
            .option("uri",mongoConfig.uri)
            .option("collection",AVERAGE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        //统计每种电影类型中评分最高的10个电影
        //需要用left join，应为只需要有评分的电影数据集
        val moviesWithScore: DataFrame = movieDF.join(averageMoviesDF,Seq("mid","mid"))

        //所有的电影类别
        val genres = List("Action", "Adventure", "Animation", "Comedy", "Ccrime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
            , "Romance", "Science", "Tv", "Thriller", "War", "Western")
        val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)


        val genrenTopMovies: DataFrame = genresRDD.cartesian(moviesWithScore.rdd).filter {
            //过滤掉不匹配的类别的电影
            case (genres, row) => row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase)
        }.map {
            // 将整个数据集的数据量减小，生成RDD[String,Iter[mid,avg]]
            case (gense, row) => {
                //将genres数据集中的相同的聚集
                (gense, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
            }
        }.groupByKey().map {
            // 通过评分的大小进行数据的排序，然后将数据映射为对象
            case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10)
                .map(item => Recommendation(item._1, item._2)))
        }.toDF()


        //把数据写到MongoDB中
        genrenTopMovies
            .write.option("uri",mongoConfig.uri)
            .option("collection",GENRES_TOP_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        spark.close()
    }

}
