import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import akka.actor._
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import akka.remote.RemoteActorRef
import com.typesafe.config.ConfigFactory

case class ConstructStringsRequest(prefixes : Vector[String])
case class ConstructStringsResponse(prefixes : Vector[String])
case class SHAWorkerResponse()

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

class ConstructionManager extends Actor
{

var remoteSender : ActorRef = null
val constructionWorkerRouter = context.actorOf(
  Props[ConstructionWorker].withRouter(RoundRobinRouter(10)), name = "ConstructionWorkerRouter")
  
def receive = {

    case ConstructStringsRequest(prefixes) => { 
      constructionWorkerRouter ! ConstructStringsRequest(prefixes)
    }
	
	case ConstructStringsResponse(responseList) => {
	remoteSender ! ConstructStringsResponse(responseList)	
	}
	
	case "initialize" => {
		remoteSender = sender
		remoteSender ! "Got it.."
	}
    
	case "The RemoteActor is alive" => {
	println("Created construction workers.. ready to serve")
	}
	
	case "Hi" => {
	println("Got a message")
	}
	case _ =>  {
	println("Unknown message")
  }

}
}
object RemoteConstructor extends App 
{
/*  val customConf = ConfigFactory.parseString("""
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
     hostname = "128.227.248.196"
     port = 2552
     maximum-frame-size = 4096000b
   }
 }
}
""")*/
 // implicit val system = ActorSystem("ConstructStringsServer", customConf)
  
 implicit val system = ActorSystem("ConstructorRemoteSystem")
 val remoteActor = system.actorOf(Props[ConstructionManager], name = "ConstructionManager")
 remoteActor ! "The RemoteActor is alive"
}