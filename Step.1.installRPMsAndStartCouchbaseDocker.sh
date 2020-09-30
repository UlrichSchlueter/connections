source ./config.sourceMe


git clone https://github.com/dominictarr/random-name.git


#sudo yum install gcc gcc-c++ python-devel python-pip cmake make
# for TLS/SSL support (optional)
sudo yum install openssl-devel


sudo yum install openssl-devel

sudo yum install podman-docker

podman run -dt --name $CBDOCKERNAME -p 8091-8096:8091-8096/tcp -p 11210-11211:11210-11211/tcp couchbase

#docker run -d --name $CBDOCKERNAME -p 8091-8096:8091-8096 -p 11210-11211:11210-11211 couchbase