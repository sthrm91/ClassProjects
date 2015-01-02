import akka.actor.Actor
import akka.actor.Props
import akka.actor._
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.Map
import akka.dispatch.Foreach


case class UpdateFollowersRequest(requestorId : String, userId : String)
case class GetTweetsForUserRequest(userId : String, lastTweetTime : Long)
case class Tweet(msg: String, ownerId: String, timestamp: Long)
case class Retweet(tweet : Tweet, retweetedBy : String)

case class GetTweetsForUserResponse(tweets: ListBuffer[Tweet])
case class PostTweet(userId: String, tweet: Tweet)
case class GetTotalUsersRequest(userId: String)
case class GetTotalTweetsRequest()
case class GetTotalTweetsResponse(totalUsers: Int, numOfUsers : Int, numOfTweets : Long)


case class Register(userId: String, followers : ListBuffer[String], user : ActorRef)
case class RegistrationComplete()

/*Number of users in each client can range upto 1 million */
class Client1(numUsers: Int, sequenceName : String) extends Actor
{
   /*import context.dispatcher
   context.system.scheduler.schedule(60*1*1000 milliseconds, 2 milliseconds, self, "Shutdown")*/
 
    def receive = 
    {
      case "Start" => {
      //Creating users for this client
      for(i <- 0 until numUsers)
      {
       context.actorOf(Props( new UserActor(sequenceName + "U"+i.toString, sequenceName,  numUsers)), name = sequenceName + "U"+i.toString)
      }
     }
    
    
    case _ => println ("Client Actor")
  }

 
}


class UserActor(userId : String, clientName : String, max : Int) extends Actor
{
  val serverActor = context.actorFor("../../serverActor")
  import context.dispatcher
  var rand = new scala.util.Random()
  var followers = new ListBuffer[String]()
  
  var start = rand.nextInt(max)
  var end = if(start + 100 < max) start + 100 else start
  start = if(start == end) start - 100 else start
  for (x <- start until end)
  {
    followers += (clientName + "U" + x.toString)
  }
  
      
  var tweetBuffer = ListBuffer[Tweet]()
  var lastTweetTime : Long = 0
  var isRegistered = false
  serverActor ! Register(userId , followers, self)
  
  var followersSet = Set(followers)
  var postFreq = new scala.util.Random().nextInt(10)
  
  context.system.scheduler.schedule(10000 milliseconds, 1000 + postFreq * 1000 milliseconds, self, "PostTweet")
  
  context.system.scheduler.schedule(10000 milliseconds, 500 milliseconds, self, "GetUpdates") 
  
  context.system.scheduler.schedule(10000 milliseconds, 50000 milliseconds, self, "FollowSomeOne") 
  
  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val r = new scala.util.Random
      val randomNum = r.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
  
  def tweetMsg(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ (' ' to '!' )
    var tweetMsg: String = randomStringFromCharList(length, chars)
    tweetMsg
    }
  
  //Using this for tweet.msg
   def tweetStr(length: Int): String = {
    val words = List("hello ", "delhi ", "states ", "#", " ", "amazing", "America ", "Obama ", "colors ", "country ", "World ", "parliament", "flag ",
        "movies ", "series ", "japan ", "china ", "india ","russia ", "europe ", "mars ", "missile ", "hollywood ", "/")
    var tweetStr = randomStringFromWordsList(length, words)
    tweetStr
    }
   
     def randomStringFromWordsList(length: Int, words: List[String]): String = {
    val sb = new StringBuilder
    for(i <- 1 to length){
      val r = new scala.util.Random
      val randomNum = r.nextInt(words.length)
      sb.append(words(randomNum))
    }
    sb.toString
  }
   def  receive =
   {
    
     case GetTweetsForUserResponse(tweets) => {
       //println("@" + userId + " received " + tweets.length + " tweets")
       lastTweetTime = if(tweets.length > 0) tweets.last.timestamp  else lastTweetTime   
       //println("@" +userId + " - no of updates " + tweets.length)
     } 
     
     case "GetUpdates" => {
       if(isRegistered)
       serverActor !  GetTweetsForUserRequest(userId, lastTweetTime)
       
     }
    
     case "PostTweet" => {
       if(isRegistered)
       serverActor ! PostTweet(userId, new Tweet(tweetStr(18), userId, System.currentTimeMillis()))
     }
     
     
     case RegistrationComplete() => {
       isRegistered = true
       //println(userId + " completed registration")
     }
     
     case "GetTweetsForUser" =>
     {
      //Total tweets by each user
       serverActor ! GetTotalTweetsRequest() 
     }
       
     case "FollowSomeOne" => {
       serverActor ! UpdateFollowersRequest(userId, clientName + "U" + rand.nextInt(max).toString)
       
     }
     
     case _ => {
       //println("User" + userId + "created")
     }  
   
   }
  
}


case class UserTweeted(usedId : String, tweet : Tweet)
case class RespondTweetsForUser(userId : String, requestor : ActorRef, lastTweetTime : Long)

/*Fixed actors for server management*/
class ServerActor() extends Actor
{
  import context.dispatcher
  context.system.scheduler.schedule(60*10*1000 milliseconds, 30*1000 milliseconds, self, "Shutdown")
    
    var userId = 1
    var followerServer = context.actorOf(Props( new FollowerServer()) )
    var tweetServer = context.actorOf(Props (new TweetServer(followerServer) ))
  
  def receive = 
  {
    case Register(userId, followers, userRef) => 
    {
        //println("@" + userId + " has followers: "+ followers.toString)
        followerServer ! Register(userId, followers, userRef)
        tweetServer ! Register(userId, new ListBuffer[String], userRef)
    }
    
    case UpdateFollowersRequest(requestorId, userId) =>
    {
        //println(userId + "requested followers")
        followerServer ! UpdateFollowersRequest(requestorId, userId)
        //tweetServer ! UpdateFollowersRequest(requestorId, userId)
    }
  
    case GetTweetsForUserRequest(userId, lastTweetTime) => {
        
        followerServer ! RespondTweetsForUser(userId, sender, lastTweetTime)        
    }
      
    case PostTweet(userId, tweet) =>
    {
      tweetServer ! PostTweet(userId, tweet)
      //println("@" + userId + " tweeted: " + tweet.msg )
    }
    
  
    
    case GetTotalTweetsResponse(totalUsers, numOfUsers, numOfTweets) => 
    {
      println("[Total Users:" + totalUsers + "] [Frequent Tweeters:" +numOfUsers + "] [Total Tweets: "+ numOfTweets+ "]")
      context.system.shutdown()
      
    }
      
    case "Shutdown" =>{
      println("Gathering statistics ! Going to shutdown soon")
      tweetServer ! GetTotalTweetsRequest()
      
    } 
  }

  
}

class FollowerServer() extends Actor 
{
  var followersByUserId = Map[String, ListBuffer[String]]()
  var updatesByUserId = Map[String, ListBuffer[Tweet]]()
  var numRegistered = 0
  
  def receive = {
    case Register(userId , followers, userRef) => {
     // println(userId + " just registered in followerServer")
      followersByUserId +=  userId -> followers
      updatesByUserId += userId -> new ListBuffer[Tweet]()
      numRegistered += 1
      userRef ! RegistrationComplete()
      if( numRegistered%1000 == 0 ) println("NumOfUsers reached : " + numRegistered)
    }
    
    case UpdateFollowersRequest(follower, following) =>
    {
      followersByUserId(following) += follower       
    }
    
    case UserTweeted(userId, tweet) => {
     
      followersByUserId(userId).foreach( follower => 
      { 
          updatesByUserId(follower) += tweet;  
          if(updatesByUserId(follower).length > 10) 
            updatesByUserId(follower).remove(0)
       })            
    }
    
    case RespondTweetsForUser(userId, sender, lastTweetTime) => {
      val buffer = updatesByUserId(userId)
      var updates = ListBuffer[Tweet]()
      
      var i = buffer.length - 1
      var isNew = true
      while(isNew && i > 0)
      {
        if(buffer(i).timestamp > lastTweetTime)
        {
          updates += buffer(i)
        }
        else
        {
          isNew = false
        }
        i -= 1
      }
      sender ! GetTweetsForUserResponse(updates)
    }
    
    case _ => { 
      println("Reached FollowerServer") 
      }
  }
}
class TweetServer (followerServer : ActorRef) extends Actor 
{
  var tweetsByUserId = Map[String, ListBuffer[Tweet]]()
  
  def receive = {
    case Register(userId, followers, userRef) => {
      //println(userId + " just registered in TweetServer")
      tweetsByUserId +=  userId -> new ListBuffer[Tweet]
      
    }
    
    case PostTweet(userId, tweet) => {
       tweetsByUserId(userId) += tweet
       followerServer ! UserTweeted(userId, tweet)
     }
    
    
        
    case GetTotalTweetsRequest() => {
      var totalTweets = 0
      var frequentUsers = 0
      var totalUsers = 0
       tweetsByUserId.foreach 
       { user => 
         totalTweets += user._2.length 
         totalUsers += 1
         if(user._2.length > 500){
           frequentUsers += 1   
         }
        }
      sender! GetTotalTweetsResponse(totalUsers, frequentUsers, totalTweets)
    }
    
    case _ => println("Reached TweetServer")
  }
  
}

object project4 extends App {
val system = ActorSystem("project4-Twitter") 
var actor : ActorRef = null

var serverActor = system.actorOf(Props (new ServerActor() ), name = "serverActor")

for(i <- 1 until 6)
{
	var clientActor1 = system.actorOf(Props (new Client1(6000, "C" + i.toString)), name = "C" + i.toString )
    clientActor1 ! "Start"
}

}