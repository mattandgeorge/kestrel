#!/bin/sh
#
# kestrel init.d script.
#

QUEUE_PATH="/var/spool/kestrel"
KESTREL_HOME="/usr/local/kestrel"
AS_USER="root"	# Probably want to create a special user..
AS_GROUP="daemon"
VERSION="1.0"
PID_FILE="/var/run/kestrel.pid"
LOG_FILE="/var/log/kestrel/kestrel.log"
OUT_FILE="/var/log/kestrel/kestrel.out"
HEAP_OPTS="-Xmx2048m -Xms1024m -XX:NewSize=256m"
# -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false 
JAVA_OPTS="-XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC $HEAP_OPTS"

# Makes the file $1 writable by the group $AS_GROUP.
function make_file_writable() {
   local filename="$1"
   touch $filename || return 1
   chgrp $AS_GROUP $filename || return 1
   chmod g+w $filename || return 1
   return 0
}

function running() {
	if [ -r $PID_FILE ]; then
		pid="$(<$PID_FILE)"
		if [ ! -z "$pid" ]; then
			local running=`ps -x $pid | awk '{print $1}' | grep $pid`
			if [ ! -z "$running" ]; then
				return 0
			fi
		fi
	fi	
	return 1
}

function find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  potential=$(ls -r1d /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default /usr/java/j* 2>/dev/null)
  for p in $potential; do
    if [ -x $p/bin/java ]; then
      JAVA_HOME=$p
      break
    fi
  done
}


# dirs under /var/run can go away between reboots.
for p in /var/run/kestrel /var/log/kestrel $QUEUE_PATH; do
  if [ ! -d $p ]; then
    mkdir -p $p
    chmod 775 $p
    chown $AS_USER $p >/dev/null 2>&1 || true
  fi
done

find_java


case "$1" in
  start)
    printf "Starting kestrel... "

    if [ ! -r $KESTREL_HOME/kestrel-$VERSION.jar ]; then
      echo "FAIL"
      echo "*** kestrel jar missing - not starting"
      exit 1
    fi
    if [ ! -x $JAVA_HOME/bin/java ]; then
      echo "FAIL"
      echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
      exit 1
    fi
    if running; then
      echo "already running."
      exit 0
    fi

		rm -f $PID_FILE
		
    if ! make_file_writable $PID_FILE; then
			echo "Could not create $PID_FILE"
			echo "FAIL"
      exit 1
		fi
		
		if ! make_file_writable $LOG_FILE; then
			echo "Could not create $LOG_FILE"
			echo "FAIL"
      exit 1
		fi
		
		if ! make_file_writable $OUT_FILE; then
			echo "Could not create $OUT_FILE"
			echo "FAIL"
      exit 1
		fi
	
    ulimit -n 8192 || echo -n " (no ulimit)"

		# Build the start up command
		java_cmd="${JAVA_HOME}/bin/java ${JAVA_OPTS} -server -verbosegc -jar ${KESTREL_HOME}/kestrel-${VERSION}.jar"
		cmd="nohup $java_cmd >>$OUT_FILE 2>&1 & echo \$! >$PID_FILE"
		#echo "cmd=$cmd"
		
		# Start up
		sudo -u $AS_USER $SHELL -c "$cmd"

    tries=0
    while ! running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;

  stop)
    printf "Stopping kestrel... "
    if ! running; then
      echo "wasn't running."
      exit 0
    fi
    
    (echo "shutdown"; sleep 2) | telnet localhost 22133 >/dev/null 2>&1
    tries=0
    while running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;
  
  status)
    if running; then
      echo "kestrel is running."
    else
      echo "kestrel is NOT running."
    fi
  ;;

  restart)
    $0 stop
    sleep 2
    $0 start
  ;;

  *)
    echo "Usage: /etc/init.d/kestrel {start|stop|restart|status}"
    exit 1
  ;;
esac

exit 0
