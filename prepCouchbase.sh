

source ./config.sourceMe
 
docker exec $CBDOCKERNAME couchbase-cli cluster-init \
    --cluster $CBCLUSTER \
    --services=data,index,query \
    --cluster-username $CBCLUSTERUSER \
    --cluster-password $CBCLUSTERPASSWORD




docker exec $CBDOCKERNAME couchbase-cli bucket-create \
    --cluster $CBCLUSTER \
    --username $CBCLUSTERUSER \
    --password $CBCLUSTERPASSWORD \
    --bucket-type couchbase \
    --bucket-ramsize 200 \
    --enable-flush 1 \
    --bucket test

    








