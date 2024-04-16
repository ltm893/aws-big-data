
import boto3

session = boto3.session.Session(profile_name='todd')
ec2_resource =  session.resource("ec2")



my_vpc = ec2_resource.Vpc('vpc-0bd6dcc70a1ceb4fb')


default_vpc = list(
            ec2_resource.vpcs.filter(
                Filters=[{"Name": "isDefault", "Values": ["true"]}]
            )
        )[0]
print(my_vpc.id)
print(default_vpc.id)




