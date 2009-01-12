/*
 * Copyright (c) 2008 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag.kestrel

import java.net.{InetAddress, InetSocketAddress, InterfaceAddress, NetworkInterface}
import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.collection.jcl
import scala.collection.mutable
import org.apache.mina.core.session.IoSession
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.SocketAcceptor
import org.apache.mina.transport.socket.nio.{NioProcessor, NioSocketAcceptor}
import net.lag.configgy.{Config, ConfigMap, Configgy, RuntimeEnvironment}
import net.lag.logging.Logger
import net.lag.naggati.IoHandlerActorAdapter


class Counter {
  private var value = new AtomicLong(0)

  def get() = value.get
  def set(n: Int) = value.set(n)
  def incr = value.addAndGet(1)
  def incr(n: Int) = value.addAndGet(n)
  def decr = value.addAndGet(-1)
  override def toString = value.get.toString
}


object KestrelStats {
  val bytesRead = new Counter
  val bytesWritten = new Counter
  val sessions = new Counter
  val totalConnections = new Counter
  val getRequests = new Counter
  val setRequests = new Counter
  val incrRequests = new Counter
  val sessionID = new Counter
}


object Kestrel {
  private val log = Logger.get
  val runtime = new RuntimeEnvironment(getClass)

  var queues: QueueCollection = null

  private val _expiryStats = new mutable.HashMap[String, Int]
  private val _startTime = Time.now

  var acceptorExecutor: ExecutorService = null
  var acceptor: SocketAcceptor = null

  private val deathSwitch = new CountDownLatch(1)

  var _clusterIndex: Option[Int] = None
  private val clusterConfigured = new CountDownLatch(1)
  def clusterIndex = { clusterConfigured.await; _clusterIndex }

  val DEFAULT_PORT = 22133


  def main(args: Array[String]): Unit = {
    runtime.load(args)
    startup(Configgy.config)
  }

  def configure(c: Option[ConfigMap]): Unit = {
    for (config <- c) {
      PersistentQueue.maxJournalSize = config.getInt("max_journal_size", 16 * 1024 * 1024)
      PersistentQueue.maxMemorySize = config.getInt("max_memory_size", 128 * 1024 * 1024)
      PersistentQueue.maxJournalOverflow = config.getInt("max_journal_overflow", 10)
    }
  }

  def startup(config: Config): Unit = {
    val listenAddress = config.getString("host", "0.0.0.0")
    val listenPort = config.getInt("port", DEFAULT_PORT)
    queues = new QueueCollection(config.getString("queue_path", "/tmp"), config.configMap("queues"))
    configure(Some(config))
    config.subscribe(configure _)

    acceptorExecutor = Executors.newCachedThreadPool()
    acceptor = new NioSocketAcceptor(acceptorExecutor, new NioProcessor(acceptorExecutor))

    // mina garbage:
    acceptor.setBacklog(1000)
    acceptor.setReuseAddress(true)
    acceptor.getSessionConfig.setTcpNoDelay(true)
    acceptor.getFilterChain.addLast("codec", new ProtocolCodecFilter(memcache.Codec.encoder,
      memcache.Codec.decoder))
    acceptor.setHandler(new IoHandlerActorAdapter((session: IoSession) => new KestrelHandler(session, config)))
    acceptor.bind(new InetSocketAddress(listenAddress, listenPort))

    val localAddress = acceptor.getLocalAddress()
    configureCluster(config, localAddress.getAddress(), localAddress.getPort())

    log.info("Kestrel started.")

    // make sure there's always one actor running so scala 2.7.2 doesn't kill off the actors library.
    actor {
      deathSwitch.await
    }
  }

  def shutdown(): Unit = {
    log.info("Shutting down!")
    queues.shutdown
    acceptor.unbind
    acceptor.dispose
    Scheduler.shutdown
    acceptorExecutor.shutdown
    // the line below causes a 1 second pause in unit tests. :(
    //acceptorExecutor.awaitTermination(5, TimeUnit.SECONDS)
    deathSwitch.countDown
  }

  def uptime() = (Time.now - _startTime) / 1000

  // if a cluster is defined, figure out where i am in it.
  def configureCluster(config: ConfigMap, myAddress: InetAddress, myPort: Int): Unit = {
    case class ClusterMember(hosts: List[InetAddress], port: Int)

    val cluster = config.getList("cluster").toList

    // build up a List[ClusterMember] of addr/ports for the cluster members.
    val members = cluster map { desc =>
      val segments = desc.split(":", 2)
      val addrList = InetAddress.getAllByName(segments(0)).toList
      ClusterMember(addrList, if (segments.length == 2) segments(1).toInt else DEFAULT_PORT)
    }
    log.debug("Cluster expands to: %s",
              members.map(m => "%s (%d)".format(m.hosts.mkString(","), m.port)).mkString("; "))

    var locals = new mutable.ListBuffer[InetAddress]
    if (myAddress.isAnyLocalAddress()) {
      // assemble a list of local addresses.
      val ifaces = NetworkInterface.getNetworkInterfaces()
      while (ifaces.hasMoreElements) {
        locals ++= jcl.Buffer(ifaces.nextElement().getInterfaceAddresses()).map(_.getAddress())
      }
    } else {
      locals += myAddress
    }

    _clusterIndex = members.zipWithIndex.filter { m =>
      (m._1.port == myPort) && (m._1.hosts.exists(locals contains _))
    }.map(_._2).firstOption
    if (_clusterIndex.isDefined) {
      log.info("In a cluster of %d, I am #%d.", cluster.size, _clusterIndex.get + 1)
    }
    clusterConfigured.countDown
  }
}
