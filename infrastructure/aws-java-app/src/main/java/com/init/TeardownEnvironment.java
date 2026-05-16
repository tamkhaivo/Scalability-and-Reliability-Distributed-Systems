package com.init;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.*;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.cognitoidentity.CognitoIdentityClient;
import software.amazon.awssdk.services.cognitoidentity.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/*
    Teardown script for the Distributed Message Ordering testing environment.
    Destroys all AWS resources tagged with Project:class-project in reverse dependency order:
        1. EC2 instances    (no dependents)
        2. EKS cluster      (depends on IAM role, but safe to delete first)
        3. Kinesis stream    (independent)
        4. IAM role/policies (depended on by EKS — must be last)

    Each step is independently try-caught so a failure in one resource
    does not block teardown of the others.

    Can be invoked standalone via main() or programmatically via run().
 */
public class TeardownEnvironment {
    private static final Logger logger = LoggerFactory.getLogger(TeardownEnvironment.class);

    private static final String CLUSTER_NAME = "MessageOrderingCluster";
    private static final String STREAM_NAME = "UserMetricsStream";
    private static final String EKS_ROLE_NAME = "MessageOrderingEksClusterRole";
    private static final String EKS_NODE_ROLE_NAME = "MessageOrderingEksNodeRole";
    private static final String EKS_ANALYTICS_POD_ROLE_NAME = "MessageOrderingAnalyticsPodRole";
    private static final String PUBLIC_PUT_ROLE_NAME = "MessageOrderingPublicPutRole";
    private static final String COGNITO_POOL_NAME = "MessageOrderingIdentityPool";
    private static final String WEB_BUCKET_NAME_PREFIX = "message-ordering-web-";
    private static final String DYNAMODB_TABLE_NAME = "UserTelemetryAggregations";
    private static final String KCL_TABLE_NAME = "analytics-processor";
    private static final String ECR_REPOSITORY_NAME = "analytics-processor";

    private static final String PROJECT_TAG_KEY = "Project";
    private static final String PROJECT_TAG_VALUE = "class-project";

    public static void main(String[] args) {
        logger.info("Starting teardown of Distributed Message Ordering testing environment...");
        boolean success = run();
        if (success) {
            logger.info("Teardown completed successfully. All resources destroyed.");
        } else {
            logger.error("Teardown completed with errors. Some resources may still be active — check AWS Console.");
            System.exit(1);
        }
    }

    /**
     * Execute full environment teardown. Returns true if all steps succeeded.
     * Called programmatically by InitializeEnvironment on rollout failure.
     */
    public static boolean run() {
        boolean allSucceeded = true;

        try (Ec2Client ec2Client = Ec2Client.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             EksClient eksClient = EksClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             KinesisClient kinesisClient = KinesisClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             IamClient iamClient = IamClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(ProfileCredentialsProvider.create()).build();
             S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             CognitoIdentityClient cognitoClient = CognitoIdentityClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             StsClient stsClient = StsClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(ProfileCredentialsProvider.create()).build();
             EcrClient ecrClient = EcrClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build()) {

            logger.info("AWS Clients initialized for teardown.");

            // Step 1: Terminate EC2 instances
            if (!teardownEC2Instances(ec2Client)) {
                allSucceeded = false;
            }

            // Step 2: Delete EKS cluster
            if (!teardownEKSCluster(eksClient)) {
                allSucceeded = false;
            }

            // Step 3: Delete Kinesis stream
            if (!teardownKinesisStream(kinesisClient)) {
                allSucceeded = false;
            }

            // Step 4: Delete IAM role and detach policies (must be last — EKS depends on it)
            if (!teardownIAMResources(iamClient)) {
                allSucceeded = false;
            }

            // Step 5: Teardown Frontend
            if (!teardownFrontendInfrastructure(s3Client, cognitoClient, stsClient)) {
                allSucceeded = false;
            }

            // Step 6: Teardown ECR
            if (!teardownECRRepository(ecrClient)) {
                allSucceeded = false;
            }

            // Step 7: Teardown DynamoDB
            if (!teardownDynamoDB(dynamoDbClient)) {
                allSucceeded = false;
            }

            // Step 8: Teardown VPC (last step)
            if (!teardownProjectVpc(ec2Client)) {
                allSucceeded = false;
            }

        } catch (Exception e) {
            logger.error("Fatal error during teardown client initialization: ", e);
            allSucceeded = false;
        }

        return allSucceeded;
    }

    // ──────────────────────────────────────────────────────────────
    //  EC2 Teardown
    // ──────────────────────────────────────────────────────────────

    private static boolean teardownEC2Instances(Ec2Client ec2Client) {
        logger.info("Teardown Step 1: Terminating EC2 instances...");
        try {
            // Find all instances tagged with Project:class-project that are not already terminated
            DescribeInstancesRequest describeRequest = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder().name("tag:" + PROJECT_TAG_KEY).values(PROJECT_TAG_VALUE).build(),
                            Filter.builder()
                                    .name("instance-state-name")
                                    .values("pending", "running", "stopping", "stopped")
                                    .build()
                    )
                    .build();

            DescribeInstancesResponse describeResponse = ec2Client.describeInstances(describeRequest);
            List<String> instanceIds = describeResponse.reservations().stream()
                    .flatMap(r -> r.instances().stream())
                    .map(Instance::instanceId)
                    .collect(Collectors.toList());

            if (instanceIds.isEmpty()) {
                logger.info("No active EC2 instances found with tag {}:{}. Skipping.", PROJECT_TAG_KEY, PROJECT_TAG_VALUE);
                return true;
            }

            logger.info("Found {} EC2 instance(s) to terminate: {}", instanceIds.size(), instanceIds);

            TerminateInstancesRequest terminateRequest = TerminateInstancesRequest.builder()
                    .instanceIds(instanceIds)
                    .build();
            ec2Client.terminateInstances(terminateRequest);

            // Wait for termination
            logger.info("Waiting for EC2 instances to reach 'terminated' state...");
            ec2Client.waiter().waitUntilInstanceTerminated(
                    DescribeInstancesRequest.builder().instanceIds(instanceIds).build()
            );

            logger.info("All EC2 instances terminated successfully.");
            return true;
        } catch (Exception e) {
            logger.error("Failed to teardown EC2 instances: ", e);
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  EKS Teardown
    // ──────────────────────────────────────────────────────────────

    private static boolean teardownEKSCluster(EksClient eksClient) {
        logger.info("Teardown Step 2: Deleting EKS cluster '{}'...", CLUSTER_NAME);
        try {
            // Check if the cluster exists first
            eksClient.describeCluster(
                    DescribeClusterRequest.builder().name(CLUSTER_NAME).build()
            );

            // Delete the cluster
            eksClient.deleteCluster(
                    DeleteClusterRequest.builder().name(CLUSTER_NAME).build()
            );

            // Poll until the cluster is fully deleted
            logger.info("Waiting for EKS cluster deletion (this may take several minutes)...");
            boolean deleted = false;
            int maxPolls = 60; // 60 * 15s = 15 minutes max
            for (int i = 0; i < maxPolls; i++) {
                try {
                    Thread.sleep(15000);
                    DescribeClusterResponse resp = eksClient.describeCluster(
                            DescribeClusterRequest.builder().name(CLUSTER_NAME).build()
                    );
                    String status = resp.cluster().statusAsString();
                    logger.info("EKS cluster status: {} (poll {}/{})", status, i + 1, maxPolls);

                    if ("DELETING".equals(status)) {
                        continue;
                    }
                } catch (software.amazon.awssdk.services.eks.model.ResourceNotFoundException e) {
                    deleted = true;
                    break;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("EKS deletion wait interrupted", ie);
                }
            }

            if (deleted) {
                logger.info("EKS cluster '{}' deleted successfully.", CLUSTER_NAME);
            } else {
                logger.warn("EKS cluster '{}' deletion timed out. May still be deleting — check console.", CLUSTER_NAME);
            }
            return true;

        } catch (software.amazon.awssdk.services.eks.model.ResourceNotFoundException e) {
            logger.info("EKS cluster '{}' does not exist. Nothing to delete.", CLUSTER_NAME);
            return true;
        } catch (Exception e) {
            logger.error("Failed to teardown EKS cluster: ", e);
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  Kinesis Teardown
    // ──────────────────────────────────────────────────────────────

    private static boolean teardownKinesisStream(KinesisClient kinesisClient) {
        logger.info("Teardown Step 3: Deleting Kinesis stream '{}'...", STREAM_NAME);
        try {
            // Verify stream exists
            kinesisClient.describeStream(
                    software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder().streamName(STREAM_NAME).build()
            );

            // Delete the stream
            kinesisClient.deleteStream(
                    DeleteStreamRequest.builder()
                            .streamName(STREAM_NAME)
                            .enforceConsumerDeletion(true) // Clean up any registered consumers
                            .build()
            );

            // Poll until stream is gone
            logger.info("Waiting for Kinesis stream deletion...");
            boolean deleted = false;
            int maxPolls = 30; // 30 * 5s = 2.5 minutes max
            for (int i = 0; i < maxPolls; i++) {
                try {
                    Thread.sleep(5000);
                    kinesisClient.describeStream(
                            software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder().streamName(STREAM_NAME).build()
                    );
                    logger.info("Kinesis stream still deleting (poll {}/{})", i + 1, maxPolls);
                } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException e) {
                    deleted = true;
                    break;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Kinesis deletion wait interrupted", ie);
                }
            }

            if (deleted) {
                logger.info("Kinesis stream '{}' deleted successfully.", STREAM_NAME);
            } else {
                logger.warn("Kinesis stream '{}' deletion timed out. Check console.", STREAM_NAME);
            }
            return true;

        } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException e) {
            logger.info("Kinesis stream '{}' does not exist. Nothing to delete.", STREAM_NAME);
            return true;
        } catch (Exception e) {
            logger.error("Failed to teardown Kinesis stream: ", e);
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────
    //  IAM Teardown
    // ──────────────────────────────────────────────────────────────

    private static boolean teardownIAMResources(IamClient iamClient) {
        logger.info("Teardown Step 4: Deleting IAM roles...");
        boolean success = true;
        
        List<String> rolesToDelete = List.of(EKS_ROLE_NAME, EKS_NODE_ROLE_NAME, EKS_ANALYTICS_POD_ROLE_NAME, PUBLIC_PUT_ROLE_NAME);
        
        for (String roleName : rolesToDelete) {
            try {
                logger.info("Cleaning up IAM role '{}'...", roleName);
                
                // Must detach all policies before the role can be deleted
                ListAttachedRolePoliciesRequest listPoliciesRequest = ListAttachedRolePoliciesRequest.builder()
                        .roleName(roleName)
                        .build();

                ListAttachedRolePoliciesResponse policiesResponse = iamClient.listAttachedRolePolicies(listPoliciesRequest);

                for (AttachedPolicy policy : policiesResponse.attachedPolicies()) {
                    logger.info("Detaching policy '{}' from role '{}'", policy.policyArn(), roleName);
                    iamClient.detachRolePolicy(
                            DetachRolePolicyRequest.builder()
                                    .roleName(roleName)
                                    .policyArn(policy.policyArn())
                                    .build()
                    );
                }

                // Delete the role
                iamClient.deleteRole(
                        DeleteRoleRequest.builder().roleName(roleName).build()
                );

                logger.info("IAM role '{}' and all attached policies removed successfully.", roleName);
            } catch (NoSuchEntityException e) {
                logger.info("IAM role '{}' does not exist. Skipping.", roleName);
            } catch (Exception e) {
                logger.error("Failed to delete IAM role '{}': ", roleName, e);
                success = false;
            }
        }
        
        teardownOidcProviders(iamClient);
        
        return success;
    }

    private static void teardownOidcProviders(IamClient iamClient) {
        logger.info("Cleaning up project-tagged IAM OIDC Providers...");
        try {
            ListOpenIdConnectProvidersResponse listResponse = iamClient.listOpenIDConnectProviders();
            for (OpenIDConnectProviderListEntry entry : listResponse.openIDConnectProviderList()) {
                // To confirm it's ours, we check tags
                GetOpenIdConnectProviderRequest getRequest = GetOpenIdConnectProviderRequest.builder()
                        .openIDConnectProviderArn(entry.arn())
                        .build();
                GetOpenIdConnectProviderResponse getResponse = iamClient.getOpenIDConnectProvider(getRequest);
                
                boolean isOurs = getResponse.tags().stream()
                        .anyMatch(t -> PROJECT_TAG_KEY.equals(t.key()) && PROJECT_TAG_VALUE.equals(t.value()));
                
                if (isOurs) {
                    logger.info("Deleting project-tagged OIDC Provider: {}", entry.arn());
                    iamClient.deleteOpenIDConnectProvider(DeleteOpenIdConnectProviderRequest.builder()
                            .openIDConnectProviderArn(entry.arn())
                            .build());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to clean up OIDC providers: ", e);
        }
    }

    private static boolean teardownFrontendInfrastructure(S3Client s3Client, CognitoIdentityClient cognitoClient, StsClient stsClient) {
        logger.info("Teardown Step 5: Cleaning up Frontend Infrastructure...");
        boolean success = true;

        // 1. S3 Bucket
        try {
            String accountId = stsClient.getCallerIdentity().account();
            String bucketName = WEB_BUCKET_NAME_PREFIX + accountId;
            
            logger.info("Cleaning up web bucket '{}'...", bucketName);
            
            // Must delete all objects before the bucket
            ListObjectsV2Response listObjects = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
            if (!listObjects.contents().isEmpty()) {
                List<ObjectIdentifier> ids = listObjects.contents().stream()
                        .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                        .collect(Collectors.toList());
                s3Client.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(software.amazon.awssdk.services.s3.model.Delete.builder().objects(ids).build())
                        .build());
            }
            
            s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            logger.info("Web bucket '{}' deleted.", bucketName);
        } catch (NoSuchBucketException e) {
            logger.info("Web bucket not found. Skipping.");
        } catch (Exception e) {
            logger.error("Failed to delete web bucket: ", e);
            success = false;
        }

        // 2. Cognito Identity Pool
        try {
            ListIdentityPoolsResponse listPools = cognitoClient.listIdentityPools(ListIdentityPoolsRequest.builder().maxResults(60).build());
            String poolId = listPools.identityPools().stream()
                    .filter(p -> COGNITO_POOL_NAME.equals(p.identityPoolName()))
                    .map(IdentityPoolShortDescription::identityPoolId)
                    .findFirst()
                    .orElse(null);

            if (poolId != null) {
                logger.info("Deleting Cognito Identity Pool: {}", poolId);
                cognitoClient.deleteIdentityPool(DeleteIdentityPoolRequest.builder().identityPoolId(poolId).build());
            }
        } catch (Exception e) {
            logger.error("Failed to delete Cognito Identity Pool: ", e);
            success = false;
        }

        return success;
    }

    private static boolean teardownECRRepository(EcrClient ecrClient) {
        logger.info("Teardown Step 6: Deleting ECR Repository '{}'...", ECR_REPOSITORY_NAME);
        try {
            ecrClient.deleteRepository(DeleteRepositoryRequest.builder()
                    .repositoryName(ECR_REPOSITORY_NAME)
                    .force(true)
                    .build());
            logger.info("ECR repository '{}' deleted successfully.", ECR_REPOSITORY_NAME);
            return true;
        } catch (software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException e) {
            logger.info("ECR repository '{}' does not exist. Skipping.", ECR_REPOSITORY_NAME);
            return true;
        } catch (Exception e) {
            logger.error("Failed to delete ECR repository: ", e);
            return false;
        }
    }

    private static boolean teardownDynamoDB(DynamoDbClient dynamoDbClient) {
        boolean success = true;
        List<String> tables = List.of(DYNAMODB_TABLE_NAME, KCL_TABLE_NAME);
        
        for (String tableName : tables) {
            logger.info("Teardown Step 7: Deleting DynamoDB table '{}'...", tableName);
            try {
                dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
                logger.info("DynamoDB table '{}' deletion initiated.", tableName);
            } catch (software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
                logger.info("DynamoDB table '{}' does not exist. Skipping.", tableName);
            } catch (Exception e) {
                logger.error("Failed to teardown DynamoDB table '{}': ", tableName, e);
                success = false;
            }
        }
        return success;
    }

    private static boolean teardownProjectVpc(Ec2Client ec2Client) {
        logger.info("Teardown Step 8: Searching for project VPC with tag {}:{}...", PROJECT_TAG_KEY, PROJECT_TAG_VALUE);
        try {
            DescribeVpcsResponse response = ec2Client.describeVpcs(DescribeVpcsRequest.builder()
                    .filters(Filter.builder().name("tag:" + PROJECT_TAG_KEY).values(PROJECT_TAG_VALUE).build())
                    .build());
            
            for (Vpc vpc : response.vpcs()) {
                VpcManager.teardownVpc(ec2Client, vpc.vpcId());
            }
            return true;
        } catch (Exception e) {
            logger.error("Failed to teardown project VPC: ", e);
            return false;
        }
    }
}
