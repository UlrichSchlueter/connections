
source ./config.sourceMe

docker stop $CBDOCKERNAME

sleep 5

docker rm $CBDOCKERNAME