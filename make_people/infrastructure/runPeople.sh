
CLUSTERID=$1
CODE=s3://ltm893-emrfs/scripts/create_objects.py
OUTPUTDIR=s3://ltm893-emrfs/output
APPNAME=SPARKPEOPLETUTORIAL1
PROFILENAME=todd

aws s3 cp create_objects.py $CODE --profile $PROFILENAME

aws emr add-steps --profile todd \
--cluster-id $CLUSTERID \
--steps Type=Spark,Name=$APPNAME,ActionOnFailure=CONTINUE,Args=$CODE,--output_uri,$OUTPUTDIR						




#aws emr describe-step --cluster-id <myClusterId> --step-id <s-1XXXXXXXXXXA>							
#Ref
# https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html
#TODOS
# get CLUSTER ID from create_emr_cluster.sh
# get Cluster.Status.State from  aws emr describe-cluster --cluster-id  --profile todd
# get Step.Status.State from aws emr add-steps