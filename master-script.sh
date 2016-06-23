sudo ~/Downloads/spark-161/sbin/start-master.sh
sudo ~/Downloads/spark-161/sbin/start-slave.sh -c 2 -m 2G spark://Spark-VM:7077
sudo ~/Downloads/zeppelin-0.5.6-incubating/bin/zeppelin-daemon.sh restart