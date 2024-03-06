import sys
import boto3

session = boto3.session.Session(profile_name='todd')
ec2 = session.resource('ec2')

def create_vpc():
    vpc_cidr = '10.30.0.0/16'
    vpc_name_value = 'spark-cluster-1'
    public_subnet_cidr = '10.30.10.0/24'

                   
    vpc = ec2.create_vpc(CidrBlock=vpc_cidr)
    # Assign a name to the VPC
    vpc.create_tags(Tags=[{"Key": "Name", "Value": vpc_name_value}])
    vpc.wait_until_available()
    print(vpc.id)

    # Create and Attach the Internet Gateway
    ig = ec2.create_internet_gateway()
    vpc.attach_internet_gateway(InternetGatewayId=ig.id)
    print(ig.id)

    # Create a route table and a public route to Internet Gateway
    route_table = vpc.create_route_table()
    route = route_table.create_route(
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=ig.id
    )
    print(route_table.id)

    # Create a Subnet
    subnet = ec2.create_subnet(CidrBlock=public_subnet_cidr, VpcId=vpc.id)
    print(subnet.id)

    # associate the route table with the subnet
    route_table.associate_with_subnet(SubnetId=subnet.id)

def delete_vpc():
    '''
    response = ec2.delete_vpc(
    
        VpcId='vpc-03532bba94be234c3',
        DryRun=False
        )

    '''
    vpc_id = 'vpc-03532bba94be234c3'
    session = boto3.session.Session(profile_name='todd')
    # ec2 = session.client('ec2')
    ec2 = session.resource('ec2')
    ec2client = ec2.meta.client

    vpc = ec2.Vpc(vpc_id)
    # detach and delete all gateways associated with the vpc
    for gw in vpc.internet_gateways.all():
        vpc.detach_internet_gateway(InternetGatewayId=gw.id)
        gw.delete()

    # delete all route table associations
    for rt in vpc.route_tables.all():
        print(rt)
        for rta in rt.associations:
            print(rta)
            if not rta.main:
                rta.delete()

    # delete any instances
    for subnet in vpc.subnets.all():
        for instance in subnet.instances.all():
            instance.terminate()

    for subnet in vpc.subnets.all():
        for interface in subnet.network_interfaces.all():
            interface.delete()
        subnet.delete()

    # finally, delete the vpc
    ec2client.delete_vpc(VpcId=vpc_id)

def delete_route_tables():
    session = boto3.session.Session(profile_name='todd')
    ec2 = session.resource('ec2')
    ec2client = ec2.meta.client
    vpc_id = 'vpc-03532bba94be234c3'
    filter = [{"Name": "vpc-id", "Values": [vpc_id]}]
    route_tables = ec2client.describe_route_tables(Filters=filter)["RouteTables"]
    for route_table in route_tables:
        for route in route_table["Routes"]:
            if route["Origin"] == "CreateRoute":
                ec2client.delete_route(
                    RouteTableId=route_table["RouteTableId"],
                    DestinationCidrBlock=route["DestinationCidrBlock"],
                )
            for association in route_table["Associations"]:
                if not association["Main"]:
                    ec2client.disassociate_route_table(
                        AssociationId=association["RouteTableAssociationId"]
                    )
                    ec2client.delete_route_table(
                        RouteTableId=route_table["RouteTableId"]
                    )
    # delete routing tables without associations
    for route_table in route_tables:
        if route_table["Associations"] == []:
            ec2client.delete_route_table(RouteTableId=route_table["RouteTableId"])
    


if __name__ == '__main__':
    delete_route_tables()