import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {
    args.toList match {
      case input :: output :: Nil =>
        // initialise spark context
        val conf = new SparkConf().setAppName("PokeSpark").setMaster("local[*]")
        val spark = SparkSession.builder.config(conf).getOrCreate()
        val df = spark.read.option("multiline", value = true).json(input)

        groupByType(df).coalesce(1)
          .write.mode("overwrite").json(f"$output/grouped_by_type")

        groupByAbilities(df).coalesce(1)
          .write.mode("overwrite").json(f"$output/grouped_by_abilities")

        aggregateSizes(df).coalesce(1)
          .write.mode("overwrite").json(f"$output/size_aggregates")

        rankStatsOverGen(df).coalesce(1)
          .write.mode("overwrite").json(f"$output/rank_stats_over_gen")

        rankStatsOverType(df).coalesce(1)
          .write.mode("overwrite").json(f"$output/rank_stats_over_type")

        spark.stop()
    }
  }

  def groupByType(df: DataFrame): DataFrame = {
    df.groupBy("type1", "type2").count()
  }

  def groupByAbilities(df: DataFrame): DataFrame = {
    val exploded = df.withColumn("ability", explode(col("abilities")))
    exploded.groupBy("ability").count()
  }

  def aggregateSizes(df: DataFrame): DataFrame = {
    df.groupBy("generation").agg(
      sum("weight_kg").alias("sum_weight"),
      avg("weight_kg").alias("avg_weight"),
      sum("height_m").alias("sum_height"),
      avg("height_m").alias("avg_height")
    )
  }

  def rankStatsOverGen(df: DataFrame): DataFrame = {
    val window = Window.partitionBy("generation").orderBy(
      col("attack").desc,
      col("defense").desc,
      col("sp_attack").desc,
      col("sp_defense").desc,
      col("speed").desc,
      col("base_total").desc
    )

    df.withColumn("rank", rank().over(window)).select(
      "rank",
      "generation",
      "name",
      "base_total",
      "attack",
      "defense",
      "sp_attack",
      "sp_defense",
      "speed"
    )
  }

  def rankStatsOverType(df: DataFrame): DataFrame = {
    val window = Window.partitionBy("type1", "type2").orderBy(
      col("attack").desc,
      col("defense").desc,
      col("sp_attack").desc,
      col("sp_defense").desc,
      col("speed").desc,
      col("base_total").desc
    )

    df.withColumn("rank", rank().over(window)).select(
      "rank",
      "type1",
      "type2",
      "name",
      "base_total",
      "attack",
      "defense",
      "sp_attack",
      "sp_defense",
      "speed"
    )
  }

}

