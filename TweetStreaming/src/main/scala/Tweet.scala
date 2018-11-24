import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken, Tweet}
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage


/**
  * La classe des tweets qui recoi un tweet sous forme de text et contient
  * la methode count words
  * @param Text le tweet
  */
class Tweets(Text : String) extends  Actor{
  val text:String = Text
  val log = Logging(context.system,this)

  /**
    * compter les mots de ce tweet
    * @return
    */
  def count_words(): Int = {
    App.tweet_traite = App.tweet_traite + 1
    return  this.text.split( "[ _,-]+").length
  }


  /**
    * methode invoquer lors de la reception d'un message
    * @return number of words
    */
  def receive = {

    case "count" ⇒ {
          log.info("received message to count words in twetter")
          println("Number of words in this tweet is :  "+this.count_words())
    }
    case _      ⇒ log.info("received unknown message")
  }

}

class BatchTweet(Group : Array[String]) extends  Actor {
  val lestweets:Array[String] = Group
  val log = Logging(context.system,this)
  val system = ActorSystem("MySystem")


  def receive = {
    case "batch" ⇒
    {
      for(i <- 0 until  lestweets.length-1){
        val myActor = system.actorOf(Props(new Tweets(lestweets(i))), name = "myactor"+i)
        myActor ! "count" 
      }
      log.info("received message to trait batch ")
    }
    case _      ⇒ log.info("received unknown message")
  }
}

object App {
  var  totnow:Int = 0
  var  totact:Int = 0
  val maxBatch:Int = 20
  var tweet_traite:Int = 0
  var list_tweet : Array[String] = Array()
  def printTweetText: PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => {
      //println(tweet.text)
      totact = totact +1
      totnow  = totnow + 1
      if(totact == maxBatch){
        println("total daba :"+totnow)
        println("total actual :"+totact)
        println("number of tweet traité:"+tweet_traite)

        println("------------------------------")
        println("------------------------------")
        val system = ActorSystem("MySystem")
        val myActor = system.actorOf(Props(new BatchTweet(list_tweet)), name = "myactor")
        myActor ! "batch"
        list_tweet = Array()
        totact = 0
      }
    }
  }
  def main(args: Array[String]): Unit = {
    //val system = ActorSystem("MySystem")
    //val aaa =  Array[String]("je suis malade","je suis malade","je suis malade","je suis malade","je suis malade","je suis malade")
    //val myActor = system.actorOf(Props(new BatchTweet(aaa)), name = "myactor")
    //myActor ! "batch"

    val consumerToken = ConsumerToken(key = "W75HPXXj6wP3vKBYVMFgTiqNL", secret = "zFEzWz6FhJWuqX0dbW1c6vgHNBfNLsb0I4X1U6n2VfyrtJXSP9")
    val accessToken = AccessToken(key = "896541932775714822-fmWjUHjXIhFw6yChmG0IbDgVw3Wc2aV", secret = "Z5pFdCtlZFsYjpsgFsswJAzsYsgf7lTs200iek87xVVxt")
    val client = TwitterStreamingClient(consumerToken, accessToken)
    client.sampleStatuses(stall_warnings = true)(printTweetText)
  }
}