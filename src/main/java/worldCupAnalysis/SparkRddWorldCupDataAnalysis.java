package worldCupAnalysis;


import com.mongodb.spark.MongoSpark;
import model.GroupPlayer;
import model.PlayersModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.sparkproject.guava.collect.Iterators;
import scala.Tuple2;

import java.util.Iterator;

public class SparkRddWorldCupDataAnalysis {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSpark")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.WordCupCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.WordCupCollection")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        JavaRDD<String> Raw_Data = sc.textFile("C:\\Users\\Murat Çetin\\Downloads\\WorldCup\\WorldCup\\WorldCupPlayers.csv");

        //data from csv to java model
        JavaRDD<PlayersModel> playersRDD = Raw_Data.map(new Function<String, PlayersModel>() {
            public PlayersModel call(String line) throws Exception {
                String[] lines = line.split(",", -1);
                return new PlayersModel(lines[0], lines[1]
                        , lines[2], lines[3], lines[4], lines[5], lines[6], lines[7], lines[8]);
            }
        });

        //How many matches did messi play on world cups?
        JavaRDD<PlayersModel> messiRDD = playersRDD.filter(new Function<PlayersModel, Boolean>() {
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getPlayerName().equals("MESSI");
            }
        });
        System.out.println(" Messi has played " + messiRDD.count()+" matches all during the world cups");


        //get Turkish players on dataset
        JavaRDD<PlayersModel> tur = playersRDD.filter(new Function<PlayersModel, Boolean>() {
            public Boolean call(PlayersModel playersModel) throws Exception {
                return playersModel.getTeam().equals("TUR");
            }
        });

        //calculate total match count for every Turkish player
        JavaPairRDD<String, String> mapRDD = tur.mapToPair(new PairFunction<PlayersModel, String, String>() {
            public Tuple2<String, String> call(PlayersModel playersModel) throws Exception {
                return new Tuple2<String, String>(playersModel.getPlayerName(), playersModel.getMatchID());
            }
        });


        JavaPairRDD<String, Iterable<String>> groupPlayer = mapRDD.groupByKey();

        JavaRDD<GroupPlayer> resultRDD = groupPlayer.map(new Function<Tuple2<String, Iterable<String>>, GroupPlayer>() {
            public GroupPlayer call(Tuple2<String, Iterable<String>> array) throws Exception {
                Iterator<String> iteratorraw = array._2().iterator();
                int size = Iterators.size(iteratorraw);
                return new GroupPlayer(array._1, size);
            }
        });


        //save to mongodb as json format belove

        /*
        {
         PlayerName : 'RUSTU',
         MatchCount : 6
        }
         */

        JavaRDD<Document> MongoRDD = resultRDD.map(new Function<GroupPlayer, Document>() {
            public Document call(GroupPlayer groupPlayer) throws Exception {
                return Document.parse("{PlayerName: " + " ' " + groupPlayer.getPlayerName() + "'"
                        + "," + "PlayerMatchCount: " + groupPlayer.getMatchCount()
                        + "}");
            }
        });

        //save to mongodb
        MongoSpark.save(MongoRDD);

    }
}
