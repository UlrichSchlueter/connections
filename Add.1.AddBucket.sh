

source ./config.sourceMe



for bucket in  ${CBADDBUCKETS//,/ }
do
    echo $bucket


        docker exec $CBDOCKERNAME couchbase-cli bucket-create \
        --cluster $CBCLUSTER \
        --username $CBCLUSTERUSER \
        --password $CBCLUSTERPASSWORD \
        --bucket-type couchbase \
        --bucket-ramsize 200 \
        --enable-flush 1 \
        --bucket $bucket
done






