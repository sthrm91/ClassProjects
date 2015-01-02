
import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import akka.actor._
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory

case class ConstructStringsRequest(prefixes : Vector[String])
case class ConstructStringsResponse(prefixes : Vector[String])
case class SHAWorkerResponse()

class SHAWorker(k : Int) extends Actor 
{
  private val sha = MessageDigest.getInstance("SHA-256")
  
  def hex_digest(s: String): String = {
    sha.digest(s.getBytes)
    .foldLeft("")((s: String, b: Byte) => s +
                  Character.forDigit((b & 0xf0) >> 4, 16) +
                  Character.forDigit(b & 0x0f, 16))
  }
  
 def startsWithK0s(x: String) : Boolean = {
    var i : Int = 0
    var returnVal: Boolean = true
    while (i < k)
    {
      if(x.charAt(i)!='0')
        returnVal=false
      i+=1
    }
    returnVal
  }
  
  def receive = {
    case ConstructStringsResponse(prefixes) =>
    {
      var acceptedStrings : Vector[String] = Vector()
      prefixes.foreach(x  => { 
        var hashStr : String = hex_digest(x)  
        if(startsWithK0s(hashStr)) 
          println(x + "\t"+ hashStr)
          })
      sender ! SHAWorkerResponse()
    }
    
    case _ => {
      println("From SHAWOrker unidentified characters")
      }
}
}


class ConstructionWorker extends Actor
{
  def getNextLengthStrings(prefix:String) : Vector[String] = {
   var asciiList = Array.range(0, 120)
   var returnList : Vector[String] = Vector()
   for (i <- 32 until 126)
      returnList +:= prefix + i.toChar
   returnList  
  }
  
  def receive = {
    case ConstructStringsRequest(prefixes) => { 
      var responseList : Vector[String] = Vector()
      prefixes.foreach(element => { 
	    responseList ++= getNextLengthStrings(element)
      } )  
      sender ! ConstructStringsResponse(responseList)
    }
    case _ =>  println("received unknown message")
  }
}

class MasterWorker(nrOfWorkers : Int, k : Int, remoteip: String) extends Actor 
{
  var constructionWorkerRouter : ActorRef = null
  if(remoteip != null)
  {
   constructionWorkerRouter = context.actorFor("akka.tcp://ConstructorRemoteSystem@" + remoteip.trim() + ":2552/user/ConstructionManager") 
  }
  val localConstructionWorkerRouter = context.actorOf(
  Props[ConstructionWorker].withRouter(RoundRobinRouter(nrOfWorkers/2)), name = "ConstructionWorkerRouter")
  
  var localsTurn : Boolean = true 
  
  var shaworkers : Vector[ActorRef] = Vector()
  for(i <- 0 until nrOfWorkers - 1)
  {
    shaworkers :+= context.actorOf(Props(new SHAWorker(k)))
  }
  val shaWorkerRouter = context.actorOf(
  Props.empty.withRouter(RoundRobinRouter(routees = shaworkers)))
  
  def receive = {
    
    case ConstructStringsRequest(prefixes) => {
      localConstructionWorkerRouter ! ConstructStringsRequest(prefixes)
      }
    
    case SHAWorkerResponse() => 
    {
                    
    }
    
    case ConstructStringsResponse(nextPrefixes) => {
      shaWorkerRouter ! ConstructStringsResponse(nextPrefixes)      
      var start:Int = 0
      var end :Int = if(nextPrefixes.size/nrOfWorkers > 0) nextPrefixes.size/nrOfWorkers - 1 else nextPrefixes.length
      var increment: Int = nextPrefixes.size/nrOfWorkers
      while (start < nextPrefixes.size)
      {
    	  var nextPrefixStrings : Vector[String] = nextPrefixes.slice(start, end)
    	  if(remoteip != null)
    	  {
    		  if(localsTurn)
    		  { 
    			  localConstructionWorkerRouter ! ConstructStringsRequest(nextPrefixStrings)
    			  localsTurn = false
    		  }
    		  else
    		  {
    			  constructionWorkerRouter ! ConstructStringsRequest(nextPrefixStrings)
    			  localsTurn = true    	    
    		  }
    	  }
    	  else
    	  {
    	     localConstructionWorkerRouter ! ConstructStringsRequest(nextPrefixStrings)
    	  }
    	  start += increment
    	  end = if(increment + end < nextPrefixes.size) end + increment else nextPrefixes.size -1
      }
    }
    
	case "startup" => {
          if(remoteip!= null)
          {
	  constructionWorkerRouter ! "Hi"
	  constructionWorkerRouter ! "initialize"
          }
	 }
	
	case "Got it.." => {
	println("Connection setup")
	}
	
    case _ =>  {
         println("received unknown message")
	}
	
  }

}


object project1 extends App 
{
  val customConf = ConfigFactory.parseString("""
akka {
  loglevel = "ERROR"
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    enabled-transports = ["akka.remote.netty.tcp"]
    #log-sent-messages = on
    #log-received-messages = on
    netty.tcp {
      hostname = "10.136.80.233"
      port = 0
      maximum-frame-size = 4096000b 
    }
  }
}
""")
   implicit val system = ActorSystem("ConstructStringsServer", customConf)
   var remoteip : String = null
   if(args.size > 1)
     remoteip = args(1)
   var master = system.actorOf(Props(new MasterWorker(8, Integer.parseInt(args(0)), remoteip)), name = "master")
   var sbuffer : Vector[String] = Vector("caturn9")
   master ! "startup"
   master ! ConstructStringsRequest(sbuffer)
}
