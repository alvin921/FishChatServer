#!/bin/bash


OS=`uname|awk '{$1=tolower($1);print $1}'`
#echo "OS=$OS"

PREFIX=""

if [ $OS != "linux" ]
then
	PREFIX=".exe"
fi
#echo "PREFIX=$PREFIX"

function help_clean {
	echo -e "$0 clean"
	echo -e "          clean exe files of gateway/msg_server/manager/router/monitor/client"
}

function help_build {
	echo -e "$0 build <nil>|server|gateway|msg_server|manager|router|monitor|client"
	echo -e "         <nil>|server: means to build all: gateway/msg_erver/manager/router/monitor/client"
}

function help_start {
	echo -e "$0 start <nil>|server|redis|mongo"
	echo -e "         <nil>|server: means to start all: msg_erver/gateway/manager/router/monitor"
}

function help_stop {
	echo -e "$0 stop  <nil>|server|redis|mongo"
	echo -e "         <nil>|server: means to stop all: msg_erver/gateway/manager/router/monitor"
}

function Usage {
	echo -e "Usage: $0 <cmd> <arg>"
	echo -e "<cmd>	: clean|build|start|stop"
	echo -e "<arg>	: <nil>|server|redis|mongo|gateway|msg_server|manager|router|monitor|client"
	echo -e "Descriptions:"
	help_clean
	help_build
	help_start
	help_stop
}

function proc_help {
case $1 in
	clean)
		help_clean ;;
	build)
		help_build ;;
	start)
		help_start ;;
	stop)
		help_stop ;;
	*)
		Usage ;;
esac
}

function clean {
		rm -f $1/$1$PREFIX
}


function proc_clean {
clean gateway
clean monitor
clean msg_server
clean router
clean manager
clean client
}

function build {
	case "$1" in
		gateway|msg_server|router|manager|monitor|client)
			echo -e "===>building $1..."
			cd $1  
			go build -v
			cd ..
			;;
	esac
}

function proc_build {
case "a$1" in
	agateway|amsg_server|arouter|amanager|amonitor|aclient)
		build $1
		;;
   aserver|a) 
		build gateway
		build msg_server
		build router
		build manager
		build monitor
		build client
		;;
	*)
		proc_help ;;
esac

}


function start {
	echo "#======================================="
	read -p "start $1?[y|n]" ANS
	case $ANS in
	    n|N|no|NO|No) exit 0  ;;
	    y|Y|yes|Yes)  ;;
	    *) ;;
	esac

	case "x$1" in
		"xmanager"|"xmonitor"|"xrouter"|"xgateway")
			./$1/$1$PREFIX -conf_file=./$1/$1.json &
			;;
		"xmsg_server")
			./$1/$1$PREFIX -conf_file=./$1/$1.19001.json &
			./$1/$1$PREFIX -conf_file=./$1/$1.19000.json &
			;;
		"xredis")
			if [ $OS == "linux" ] 
			then
				sudo /etc/init.d/redis_6379 start
			else
				net start redis
			fi
			;;
		"xmongo")
			if [ $OS == "linux" ] 
			then
				$DIR=$HOME/RDAWatchServer
				if [ ! -d $DIR/db ]; then
					mkdir $DIR/db
				fi
				
				mongod --dbpath=$DIR/db --storageEngine=mmapv1 --logpath=$DIR/mongod.log --logappend --fork &
			else
				net start mongodb
			fi
			;;
	esac
}

function proc_start {
case "x$1" in
	"x"|"xserver")
		start msg_server
		start gateway
		start router
		start manager
		start monitor
		;;
	"xredis"|"xmongo")
		start $1
		;;
	*)
		proc_help ;;
esac

}

function stop {
	pids=`ps -ef | grep $1 | awk '{print $2}'`
	for item in ${pids[*]}; do
		echo "kill $1:$item"
		kill -9 $item
	done
}

function proc_stop {
case "x$1" in
	"x"|"xserver")
		stop monitor
		stop manager
		stop router
		stop gateway
		stop msg_server
		;;
	"xmanager"|"xmonitor"|"xrouter"|"xgateway"|"xmsg_server")
		stop $1
		;;
	"xredis")
		if [ $OS == "linux" ] 
		then
			sudo /etc/init.d/redis_6379 stop
		else
			net stop redis
		fi
		;;
	"xmongo")
		if [ $OS == "linux" ] 
		then
			stop mongod
		else
			net stop mongodb
		fi
		;;
	*)
		proc_help ;;
esac
}

function status {
	pids=`ps -ef | grep $1 | awk '{print $2}'`
	for item in ${pids[*]}; do
		echo "$1:$item"
	done
}

function proc_status {
case "x$1" in
	"x"|"xserver")
		status monitor
		status manager
		status router
		status gateway
		status msg_server
		;;
	"xmanager"|"xmonitor"|"xrouter"|"xgateway"|"xmsg_server")
		status $1
		;;
	"xredis")
		#if [ $OS == "linux" ] 
		#then
		#else
		#fi
		;;
	"xmongo")
		#if [ $OS == "linux" ] 
		#then
		#else
		#fi
		;;
esac
}

case "$1" in 
	clean)
		proc_clean $2 ;;
	build)
		proc_build $2 ;;
	start)
		proc_start $2 ;;
	stop)
		proc_stop $2 ;;
	status)
		proc_status $2 ;;
	help)
		proc_help $2 ;;
	*)
		proc_help;;
esac
