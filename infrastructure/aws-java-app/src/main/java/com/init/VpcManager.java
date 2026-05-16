package com.init;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages the custom VPC infrastructure for the EKS environment.
 * Handles creation and teardown of VPCs, Subnets, IGs, and Route Tables.
 */
public class VpcManager {
    private static final Logger logger = LoggerFactory.getLogger(VpcManager.class);
    
    private static final String VPC_CIDR = "10.0.0.0/16";
    private static final String PROJECT_TAG_KEY = "Project";
    private static final String PROJECT_TAG_VALUE = "class-project";

    /**
     * Creates a new VPC and required networking components for EKS.
     * Returns the VPC ID.
     */
    public static String createEksVpc(Ec2Client ec2Client) {
        logger.info("Creating custom EKS VPC...");

        // 1. Create VPC
        CreateVpcResponse vpcResponse = ec2Client.createVpc(CreateVpcRequest.builder()
                .cidrBlock(VPC_CIDR)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.VPC)
                        .tags(Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build(),
                              Tag.builder().key("Name").value("EksCustomVpc").build())
                        .build())
                .build());
        
        String vpcId = vpcResponse.vpc().vpcId();
        logger.info("VPC created: {}", vpcId);

        // Wait for VPC to be available
        ec2Client.waiter().waitUntilVpcAvailable(DescribeVpcsRequest.builder().vpcIds(vpcId).build());

        // 2. Enable DNS Support/Hostnames
        ec2Client.modifyVpcAttribute(ModifyVpcAttributeRequest.builder()
                .vpcId(vpcId)
                .enableDnsSupport(AttributeBooleanValue.builder().value(true).build())
                .build());
        ec2Client.modifyVpcAttribute(ModifyVpcAttributeRequest.builder()
                .vpcId(vpcId)
                .enableDnsHostnames(AttributeBooleanValue.builder().value(true).build())
                .build());

        // 3. Create Internet Gateway
        CreateInternetGatewayResponse igResponse = ec2Client.createInternetGateway(CreateInternetGatewayRequest.builder()
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INTERNET_GATEWAY)
                        .tags(Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                        .build())
                .build());
        String igId = igResponse.internetGateway().internetGatewayId();
        ec2Client.attachInternetGateway(AttachInternetGatewayRequest.builder()
                .vpcId(vpcId)
                .internetGatewayId(igId)
                .build());
        logger.info("Internet Gateway attached: {}", igId);

        // 4. Create Subnets (simplified: 2 public subnets)
        String[] azs = {"us-east-1a", "us-east-1b"};
        String[] cidrs = {"10.0.1.0/24", "10.0.2.0/24"};
        
        for (int i = 0; i < azs.length; i++) {
            CreateSubnetResponse subnetResponse = ec2Client.createSubnet(CreateSubnetRequest.builder()
                    .vpcId(vpcId)
                    .cidrBlock(cidrs[i])
                    .availabilityZone(azs[i])
                    .tagSpecifications(TagSpecification.builder()
                            .resourceType(ResourceType.SUBNET)
                            .tags(Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build(),
                                  Tag.builder().key("kubernetes.io/role/elb").value("1").build(), // Tag for ALB
                                  Tag.builder().key("Name").value("EksPublicSubnet-" + azs[i]).build())
                            .build())
                    .build());
            String subnetId = subnetResponse.subnet().subnetId();
            
            // Auto-assign public IP
            ec2Client.modifySubnetAttribute(ModifySubnetAttributeRequest.builder()
                    .subnetId(subnetId)
                    .mapPublicIpOnLaunch(AttributeBooleanValue.builder().value(true).build())
                    .build());
            
            logger.info("Subnet created: {} in {}", subnetId, azs[i]);
        }

        // 5. Setup Route Table
        CreateRouteTableResponse rtResponse = ec2Client.createRouteTable(CreateRouteTableRequest.builder()
                .vpcId(vpcId)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.ROUTE_TABLE)
                        .tags(Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                        .build())
                .build());
        String rtId = rtResponse.routeTable().routeTableId();
        
        ec2Client.createRoute(CreateRouteRequest.builder()
                .routeTableId(rtId)
                .destinationCidrBlock("0.0.0.0/0")
                .gatewayId(igId)
                .build());
        
        // Associate RT with subnets
        DescribeSubnetsResponse subnets = ec2Client.describeSubnets(DescribeSubnetsRequest.builder()
                .filters(Filter.builder().name("vpc-id").values(vpcId).build()).build());
        
        for (Subnet s : subnets.subnets()) {
            ec2Client.associateRouteTable(AssociateRouteTableRequest.builder()
                    .routeTableId(rtId)
                    .subnetId(s.subnetId())
                    .build());
        }

        return vpcId;
    }

    /**
     * Completely destroys a VPC and all its project-tagged dependencies.
     */
    public static void teardownVpc(Ec2Client ec2Client, String vpcId) {
        logger.info("Tearing down VPC: {}", vpcId);
        
        try {
            // 1. Delete NAT Gateways
            DescribeNatGatewaysResponse natRes = ec2Client.describeNatGateways(DescribeNatGatewaysRequest.builder()
                    .filter(Filter.builder().name("vpc-id").values(vpcId).build()).build());
            for (NatGateway ng : natRes.natGateways()) {
                if (!"deleted".equals(ng.stateAsString())) {
                    ec2Client.deleteNatGateway(DeleteNatGatewayRequest.builder().natGatewayId(ng.natGatewayId()).build());
                    logger.info("Deleting NAT Gateway: {}", ng.natGatewayId());
                }
            }
            // Wait for NAT deletion (they take time)
            if (!natRes.natGateways().isEmpty()) {
                logger.info("Waiting for NAT Gateways to delete...");
                Thread.sleep(30000);
            }

            // 2. Detach and Delete Internet Gateways
            DescribeInternetGatewaysResponse igRes = ec2Client.describeInternetGateways(DescribeInternetGatewaysRequest.builder()
                    .filters(Filter.builder().name("attachment.vpc-id").values(vpcId).build()).build());
            for (InternetGateway ig : igRes.internetGateways()) {
                ec2Client.detachInternetGateway(DetachInternetGatewayRequest.builder()
                        .internetGatewayId(ig.internetGatewayId()).vpcId(vpcId).build());
                ec2Client.deleteInternetGateway(DeleteInternetGatewayRequest.builder()
                        .internetGatewayId(ig.internetGatewayId()).build());
                logger.info("Deleted Internet Gateway: {}", ig.internetGatewayId());
            }

            // 3. Delete Subnets
            DescribeSubnetsResponse subRes = ec2Client.describeSubnets(DescribeSubnetsRequest.builder()
                    .filters(Filter.builder().name("vpc-id").values(vpcId).build()).build());
            for (Subnet s : subRes.subnets()) {
                ec2Client.deleteSubnet(DeleteSubnetRequest.builder().subnetId(s.subnetId()).build());
                logger.info("Deleted Subnet: {}", s.subnetId());
            }

            // 4. Delete Route Tables (except main)
            DescribeRouteTablesResponse rtRes = ec2Client.describeRouteTables(DescribeRouteTablesRequest.builder()
                    .filters(Filter.builder().name("vpc-id").values(vpcId).build()).build());
            for (RouteTable rt : rtRes.routeTables()) {
                boolean isMain = rt.associations().stream().anyMatch(RouteTableAssociation::main);
                if (!isMain) {
                    ec2Client.deleteRouteTable(DeleteRouteTableRequest.builder().routeTableId(rt.routeTableId()).build());
                    logger.info("Deleted Route Table: {}", rt.routeTableId());
                }
            }

            // 5. Delete Security Groups (except default)
            DescribeSecurityGroupsResponse sgRes = ec2Client.describeSecurityGroups(DescribeSecurityGroupsRequest.builder()
                    .filters(Filter.builder().name("vpc-id").values(vpcId).build()).build());
            for (SecurityGroup sg : sgRes.securityGroups()) {
                if (!"default".equals(sg.groupName())) {
                    ec2Client.deleteSecurityGroup(DeleteSecurityGroupRequest.builder().groupId(sg.groupId()).build());
                    logger.info("Deleted Security Group: {}", sg.groupId());
                }
            }

            // 6. Finally Delete VPC
            ec2Client.deleteVpc(DeleteVpcRequest.builder().vpcId(vpcId).build());
            logger.info("VPC {} deleted successfully.", vpcId);

        } catch (Exception e) {
            logger.error("Error during VPC teardown: ", e);
        }
    }

    /**
     * Standalone main for manual teardown of a specific VPC.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: VpcManager <region> <vpc-id>");
            return;
        }
        Region region = Region.of(args[0]);
        String vpcId = args[1];

        try (Ec2Client ec2Client = Ec2Client.builder().region(region).credentialsProvider(ProfileCredentialsProvider.create()).build()) {
            teardownVpc(ec2Client, vpcId);
        }
    }
}
