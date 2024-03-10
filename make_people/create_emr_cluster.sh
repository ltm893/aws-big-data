
PROFILENAME=todd
LOGDIR=s3://ltm893-emrfs/logs

datecmd() {
   arr=("$@")
   now=`date +"%Y-%m-%dT%H:%M:%S%:z"`
   echo $now ${arr[*]}
}


CLUSTERID=`aws emr create-cluster --profile $PROFILENAME --name "EMR1" --release-label emr-5.36.1	--applications Name=Spark  --log-uri $LOGDIR \
        --ec2-attributes KeyName=Sys0 --instance-type m5.xlarge --instance-count 3 --use-default-roles   | jq -r '.ClusterId' `

echo $CLUSTERID

CLUSTERSTATE=`aws emr describe-cluster --cluster-id $CLUSTERID --profile $PROFILENAME | jq '.Cluster.Status.State'`

echo "CLUSTERSTATE" $CLUSTERSTATE

while [[ "$CLUSTERSTATE" != "WAITING" ]] 

do
    sleep 60
    CLUSTERSTATE=`aws emr describe-cluster --cluster-id $CLUSTERID --profile $PROFILENAME | jq '.Cluster.Status.State'`
        if [[ "CLUSTERSTATE" == 'WAITING' ]]
            then
                echo "CLUSTERSTATE" $CLUSTERSTATE "BREAKING OUT"
                break
        fi

done 


#datecmd "${CMD[@]}"
#"${CMD[@]}"
