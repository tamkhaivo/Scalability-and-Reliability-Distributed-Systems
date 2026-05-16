package com.init;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.eks.model.*;
import software.amazon.awssdk.services.eks.model.Logging;
import software.amazon.awssdk.services.eks.model.ComputeConfigRequest;
import software.amazon.awssdk.services.eks.model.StorageConfigRequest;
import software.amazon.awssdk.services.eks.model.CreateAccessConfigRequest;
import software.amazon.awssdk.services.eks.model.KubernetesNetworkConfigRequest;
import software.amazon.awssdk.services.eks.model.BlockStorage;
import software.amazon.awssdk.services.eks.model.ElasticLoadBalancing;
import software.amazon.awssdk.services.eks.model.LogSetup;
import software.amazon.awssdk.services.eks.model.LogType;
import software.amazon.awssdk.services.eks.model.AuthenticationMode;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.cognitoidentity.CognitoIdentityClient;
import software.amazon.awssdk.services.cognitoidentity.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Base64;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/*
    Initialize the environment for the distributed message ordering system.
    This class will be used to create and configure the AWS resources required for the system.

    Resource Required:
        - IAM Policies and Roles to Create Users, Policies, Roles for EKS, S3, and Kinesis.
        - EKS cluster to deploy EC2 container which will READ and Digest the Kinesis Data Stream for User Metrics.
        - Kinesis Data Stream to collect user metrics
        - EC2 instance to produce fake user metrics 
        - EC2 instance to collect and output final user metrics 

    Reliability:
        - Every provisioning step is wrapped in RetryExecutor with exponential backoff (3 attempts).
        - If retries are exhausted at any step, TeardownEnvironment.run() is invoked to
          destroy all project-tagged resources before the process exits.
        - EC2 instance launch is idempotent — skips if instances already exist with the project tag.
 */
public class InitializeEnvironment {
    private static final Logger logger = LoggerFactory.getLogger(InitializeEnvironment.class);
    
    private static final String CLUSTER_NAME = "MessageOrderingCluster";
    private static final String STREAM_NAME = "UserMetricsStream";
    private static final String EKS_ROLE_NAME = "MessageOrderingEksClusterRole";
    private static final String EKS_NODE_ROLE_NAME = "MessageOrderingEksNodeRole";
    private static final String EKS_ANALYTICS_POD_ROLE_NAME = "MessageOrderingAnalyticsPodRole";
    private static final String PUBLIC_PUT_ROLE_NAME = "MessageOrderingPublicPutRole";
    private static final String PRODUCER_ROLE_NAME = "MessageOrderingProducerRole";
    private static final String PRODUCER_PROFILE_NAME = "MessageOrderingProducerProfile";
    private static final String COGNITO_POOL_NAME = "MessageOrderingIdentityPool";
    private static final String WEB_BUCKET_NAME_PREFIX = "message-ordering-web-";
    private static final String DYNAMODB_TABLE_NAME = "UserTelemetryAggregations";
    private static final String ECR_REPOSITORY_NAME = "analytics-processor";

    private static final String PROJECT_TAG_KEY = "Project";
    private static final String PROJECT_TAG_VALUE = "class-project";

    private static final int MAX_RETRY_ATTEMPTS = 3;

    public static void main(String[] args) {
        logger.info("Starting deployment of Distributed Message Ordering testing environment...");

        try (IamClient iamClient = IamClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(ProfileCredentialsProvider.create()).build();
             EksClient eksClient = EksClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             KinesisClient kinesisClient = KinesisClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             Ec2Client ec2Client = Ec2Client.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             CognitoIdentityClient cognitoClient = CognitoIdentityClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             StsClient stsClient = StsClient.builder().region(Region.AWS_GLOBAL).credentialsProvider(ProfileCredentialsProvider.create()).build();
             EcrClient ecrClient = EcrClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build();
             DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).credentialsProvider(ProfileCredentialsProvider.create()).build()) {

            logger.info("AWS Clients initialized successfully.");

            java.util.Map<String, String> eksRoleArns = RetryExecutor.execute(
                    () -> setupIAMResources(iamClient),
                    "IAM Setup",
                    MAX_RETRY_ATTEMPTS
            );

            String clusterRoleArn = eksRoleArns.get("clusterRoleArn");
            String nodeRoleArn = eksRoleArns.get("nodeRoleArn");

            RetryExecutor.executeVoid(
                    () -> setupKinesisStream(kinesisClient),
                    "Kinesis Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupDynamoDB(dynamoDbClient),
                    "DynamoDB Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupECRRepository(ecrClient),
                    "ECR Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupEKSCluster(eksClient, ec2Client, clusterRoleArn, nodeRoleArn),
                    "EKS Cluster Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupEC2Instances(ec2Client),
                    "EC2 Instance Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupIRSA(iamClient, eksClient),
                    "EKS IRSA Setup",
                    MAX_RETRY_ATTEMPTS
            );

            RetryExecutor.executeVoid(
                    () -> setupFrontendInfrastructure(s3Client, cognitoClient, iamClient, stsClient, kinesisClient),
                    "Frontend Infrastructure Setup",
                    MAX_RETRY_ATTEMPTS
            );

            logger.info("Deployment script execution completed. Please check AWS Console for resource provisioning status.");

        } catch (RetryExecutor.RetryExhaustedException e) {
            logger.error("Rollout FAILED after exhausting all retries. Error: {}", e.getMessage(), e);
            TeardownEnvironment.run();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Unexpected error during environment initialization. Error: {}", e.getMessage(), e);
            TeardownEnvironment.run();
            System.exit(1);
        }
    }

    private static java.util.Map<String, String> setupIAMResources(IamClient iamClient) {
        logger.info("Step 1: Setting up IAM Resources...");

        String clusterRoleArn = createClusterRole(iamClient);
        String nodeRoleArn = createNodeRole(iamClient);
        setupProducerIAM(iamClient);

        return java.util.Map.of(
            "clusterRoleArn", clusterRoleArn,
            "nodeRoleArn", nodeRoleArn
        );
    }

    private static String createClusterRole(IamClient iamClient) {
        String trustPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"Service\": \"eks.amazonaws.com\"},"
                + "\"Action\": \"sts:AssumeRole\""
                + "}]"
                + "}";

        try {
            GetRoleRequest getRoleRequest = GetRoleRequest.builder().roleName(EKS_ROLE_NAME).build();
            GetRoleResponse roleResponse = iamClient.getRole(getRoleRequest);
            logger.info("Cluster Role '{}' already exists: {}", EKS_ROLE_NAME, roleResponse.role().arn());
            return roleResponse.role().arn();
        } catch (NoSuchEntityException e) {
            logger.info("Creating new IAM Role for EKS Cluster: {}", EKS_ROLE_NAME);
            CreateRoleRequest createRoleRequest = CreateRoleRequest.builder()
                    .roleName(EKS_ROLE_NAME)
                    .assumeRolePolicyDocument(trustPolicy)
                    .description("Role for EKS Cluster to manage resources")
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build();
            
            CreateRoleResponse createRoleResponse = iamClient.createRole(createRoleRequest);
            String roleArn = createRoleResponse.role().arn();

            List<String> policies = Arrays.asList(
                "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy",
                "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
            );

            for (String policyArn : policies) {
                iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                        .roleName(EKS_ROLE_NAME)
                        .policyArn(policyArn)
                        .build());
            }
            
            waitForIAMRolePropagation(iamClient, EKS_ROLE_NAME);
            return roleArn;
        }
    }

    private static String createNodeRole(IamClient iamClient) {
        String trustPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"Service\": \"ec2.amazonaws.com\"},"
                + "\"Action\": \"sts:AssumeRole\""
                + "}]"
                + "}";

        try {
            GetRoleRequest getRoleRequest = GetRoleRequest.builder().roleName(EKS_NODE_ROLE_NAME).build();
            GetRoleResponse roleResponse = iamClient.getRole(getRoleRequest);
            logger.info("Node Role '{}' already exists: {}", EKS_NODE_ROLE_NAME, roleResponse.role().arn());
            return roleResponse.role().arn();
        } catch (NoSuchEntityException e) {
            logger.info("Creating new IAM Role for EKS Nodes: {}", EKS_NODE_ROLE_NAME);
            CreateRoleRequest createRoleRequest = CreateRoleRequest.builder()
                    .roleName(EKS_NODE_ROLE_NAME)
                    .assumeRolePolicyDocument(trustPolicy)
                    .description("Role for EKS Nodes (Auto Mode)")
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build();
            
            CreateRoleResponse createRoleResponse = iamClient.createRole(createRoleRequest);
            String roleArn = createRoleResponse.role().arn();

            List<String> policies = Arrays.asList(
                "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
                "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
                "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
            );

            for (String policyArn : policies) {
                iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                        .roleName(EKS_NODE_ROLE_NAME)
                        .policyArn(policyArn)
                        .build());
            }
            
            waitForIAMRolePropagation(iamClient, EKS_NODE_ROLE_NAME);
            return roleArn;
        }
    }

    /**
     * Polls IAM GetRole until the role is consistently visible, replacing the old
     * hardcoded Thread.sleep(10000). Times out after 30 seconds.
     */
    private static void waitForIAMRolePropagation(IamClient iamClient, String roleName) {
        logger.info("Waiting for IAM role '{}' to propagate...", roleName);
        int maxPolls = 15;        // 15 * 2s = 30s max
        long pollIntervalMs = 2000;

        for (int i = 0; i < maxPolls; i++) {
            try {
                Thread.sleep(pollIntervalMs);
                iamClient.getRole(GetRoleRequest.builder().roleName(roleName).build());
                logger.info("IAM role '{}' propagated successfully.", roleName);
                return;
            } catch (NoSuchEntityException e) {
                logger.info("IAM role not yet visible (poll {}/{})", i + 1, maxPolls);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("IAM propagation wait interrupted", ie);
            }
        }

        logger.warn("IAM role propagation polling timed out after {}ms. Proceeding — downstream steps will retry if needed.",
                maxPolls * pollIntervalMs);
    }

    private static void setupKinesisStream(KinesisClient kinesisClient) {
        logger.info("Step 2: Setting up Kinesis Data Stream...");

        try {
            software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest describeStreamRequest = software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .build();
            kinesisClient.describeStream(describeStreamRequest);
            logger.info("Kinesis stream '{}' already exists.", STREAM_NAME);
        } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException e) {
            logger.info("Creating new Kinesis stream: {}", STREAM_NAME);
            CreateStreamRequest createStreamRequest = CreateStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .shardCount(2) // 2 Shards for high throughput user metrics
                    .build();
            
            kinesisClient.createStream(createStreamRequest);
            
            // Add tag to Kinesis stream
            AddTagsToStreamRequest addTagsRequest = AddTagsToStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .tags(java.util.Map.of(PROJECT_TAG_KEY, PROJECT_TAG_VALUE))
                    .build();
            kinesisClient.addTagsToStream(addTagsRequest);
            
            logger.info("Kinesis stream creation initiated for '{}' with {} tags.", STREAM_NAME, PROJECT_TAG_VALUE);
        }
    }

    private static void setupDynamoDB(DynamoDbClient dynamoDbClient) {
        logger.info("Setting up DynamoDB Table...");

        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder()
                    .tableName(DYNAMODB_TABLE_NAME)
                    .build());
            logger.info("DynamoDB table '{}' already exists.", DYNAMODB_TABLE_NAME);
        } catch (software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException e) {
            logger.info("Creating new DynamoDB table: {}", DYNAMODB_TABLE_NAME);
            dynamoDbClient.createTable(CreateTableRequest.builder()
                    .tableName(DYNAMODB_TABLE_NAME)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("window_timestamp").keyType(software.amazon.awssdk.services.dynamodb.model.KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("metric_type").keyType(software.amazon.awssdk.services.dynamodb.model.KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("window_timestamp").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("metric_type").attributeType(ScalarAttributeType.S).build()
                    )
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .tags(software.amazon.awssdk.services.dynamodb.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build());
            logger.info("DynamoDB table creation initiated for '{}'.", DYNAMODB_TABLE_NAME);
        }
    }
    private static void setupEKSCluster(EksClient eksClient, Ec2Client ec2Client, String clusterRoleArn, String nodeRoleArn) {
        logger.info("Step 3: Setting up EKS Cluster with Auto Mode...");

        try {
            DescribeClusterRequest describeClusterRequest = DescribeClusterRequest.builder()
                    .name(CLUSTER_NAME)
                    .build();
            eksClient.describeCluster(describeClusterRequest);
            logger.info("EKS Cluster '{}' already exists.", CLUSTER_NAME);
        } catch (software.amazon.awssdk.services.eks.model.ResourceNotFoundException e) {
            logger.info("EKS Cluster not found. Identifying or creating project VPC...");

            // 1. Check for existing project VPC
            DescribeVpcsResponse vpcsResponse = ec2Client.describeVpcs(DescribeVpcsRequest.builder()
                    .filters(Filter.builder().name("tag:" + PROJECT_TAG_KEY).values(PROJECT_TAG_VALUE).build())
                    .build());
            
            String vpcId;
            if (!vpcsResponse.vpcs().isEmpty()) {
                vpcId = vpcsResponse.vpcs().get(0).vpcId();
                logger.info("Found existing project VPC: {}", vpcId);
            } else {
                vpcId = VpcManager.createEksVpc(ec2Client);
            }

            // 2. Identify subnets in that VPC
            DescribeSubnetsRequest subnetsRequest = DescribeSubnetsRequest.builder()
                    .filters(Filter.builder().name("vpc-id").values(vpcId).build())
                    .build();
            DescribeSubnetsResponse subnetsResponse = ec2Client.describeSubnets(subnetsRequest);
            
            List<String> subnetIds = subnetsResponse.subnets().stream()
                    .map(Subnet::subnetId)
                    .collect(Collectors.toList());
            
            logger.info("Using subnets: {}", subnetIds);

            VpcConfigRequest vpcConfig = VpcConfigRequest.builder()
                    .subnetIds(subnetIds)
                    .build();

            CreateClusterRequest createClusterRequest = CreateClusterRequest.builder()
                    .name(CLUSTER_NAME)
                    .roleArn(clusterRoleArn)
                    .resourcesVpcConfig(vpcConfig)
                    .version("1.31")
                    .accessConfig(CreateAccessConfigRequest.builder()
                            .authenticationMode(AuthenticationMode.API)
                            .bootstrapClusterCreatorAdminPermissions(true)
                            .build())
                    .computeConfig(ComputeConfigRequest.builder()
                            .enabled(true)
                            .nodeRoleArn(nodeRoleArn)
                            .nodePools("general-purpose", "system")
                            .build())
                    .storageConfig(StorageConfigRequest.builder()
                            .blockStorage(BlockStorage.builder().enabled(true).build())
                            .build())
                    .kubernetesNetworkConfig(KubernetesNetworkConfigRequest.builder()
                            .elasticLoadBalancing(ElasticLoadBalancing.builder().enabled(true).build())
                            .build())
                    .logging(Logging.builder()
                            .clusterLogging(LogSetup.builder()
                                    .types(LogType.API, LogType.AUDIT, LogType.AUTHENTICATOR, LogType.CONTROLLER_MANAGER, LogType.SCHEDULER)
                                    .enabled(true)
                                    .build())
                            .build())
                    .tags(java.util.Map.of(PROJECT_TAG_KEY, PROJECT_TAG_VALUE))
                    .build();

            eksClient.createCluster(createClusterRequest);
            logger.info("EKS cluster creation initiated with Auto Mode. Note: Cluster creation takes 10-15 minutes.");
        }
    }

    private static void setupIRSA(IamClient iamClient, EksClient eksClient) {
        logger.info("Step 5: Setting up IAM Roles for Service Accounts (IRSA)...");

        // 1. Wait for cluster to be ACTIVE to get OIDC issuer
        Cluster cluster = waitForClusterActive(eksClient, CLUSTER_NAME);
        String oidcIssuer = cluster.identity().oidc().issuer();
        String oidcUrl = oidcIssuer.replace("https://", "");
        
        logger.info("Cluster OIDC Issuer: {}", oidcIssuer);

        // 2. Create IAM OIDC Provider
        String oidcProviderArn = setupOidcProvider(iamClient, oidcIssuer);
        
        // 3. Create Pod IAM Role
        String podRoleArn = setupPodRole(iamClient, oidcUrl, oidcProviderArn);

        // 4. Update k8s service account with the correct role ARN
        writeK8sServiceAccount(podRoleArn);
    }

    private static void writeK8sServiceAccount(String roleArn) {
        String saPath = "../../k8s/serviceaccount.yaml";
        logger.info("Updating K8s ServiceAccount at {} with role: {}", saPath, roleArn);
        
        String yaml = "apiVersion: v1\n" +
                "kind: ServiceAccount\n" +
                "metadata:\n" +
                "  name: analytics-consumer\n" +
                "  namespace: default\n" +
                "  annotations:\n" +
                "    eks.amazonaws.com/role-arn: " + roleArn + "\n";

        try {
            java.io.File file = new java.io.File(saPath);
            if (file.exists()) {
                try (FileWriter writer = new FileWriter(file)) {
                    writer.write(yaml);
                }
                logger.info("K8s ServiceAccount updated successfully.");
            } else {
                logger.warn("ServiceAccount file not found at {}. Skipping update.", saPath);
            }
        } catch (IOException e) {
            logger.error("Failed to update K8s ServiceAccount: ", e);
        }
    }

    private static Cluster waitForClusterActive(EksClient eksClient, String clusterName) {
        logger.info("Waiting for EKS cluster '{}' to become ACTIVE...", clusterName);
        int maxPolls = 60; // 60 * 20s = 20 minutes
        for (int i = 0; i < maxPolls; i++) {
            DescribeClusterResponse response = eksClient.describeCluster(
                    DescribeClusterRequest.builder().name(clusterName).build()
            );
            Cluster cluster = response.cluster();
            String status = cluster.statusAsString();
            logger.info("Cluster status: {} (poll {}/{})", status, i + 1, maxPolls);

            if ("ACTIVE".equals(status)) {
                return cluster;
            }
            if ("FAILED".equals(status)) {
                throw new RuntimeException("EKS Cluster creation FAILED");
            }

            try { Thread.sleep(20000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        throw new RuntimeException("Timed out waiting for EKS cluster to become ACTIVE");
    }

    private static String setupOidcProvider(IamClient iamClient, String oidcIssuer) {
        // Find existing OIDC provider to avoid duplicates
        ListOpenIdConnectProvidersResponse listProviders = iamClient.listOpenIDConnectProviders();
        String oidcUrl = oidcIssuer.replace("https://", "");
        
        for (OpenIDConnectProviderListEntry entry : listProviders.openIDConnectProviderList()) {
            if (entry.arn().contains(oidcUrl)) {
                logger.info("OIDC Provider already exists: {}", entry.arn());
                return entry.arn();
            }
        }

        logger.info("Creating new IAM OIDC Provider for {}", oidcIssuer);
        // Note: For EKS OIDC issuers, the thumbprint is often not strictly checked by IAM anymore, 
        // but the SDK requires at least one. We use a well-known thumbprint or empty.
        // Actually, let's use the root CA thumbprint for Amazon (9E99A48A9960B14926BB7F3B02E22DA2B0AB7280)
        CreateOpenIdConnectProviderRequest request = CreateOpenIdConnectProviderRequest.builder()
                .url(oidcIssuer)
                .thumbprintList("9E99A48A9960B14926BB7F3B02E22DA2B0AB7280")
                .clientIDList("sts.amazonaws.com")
                .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                .build();

        CreateOpenIdConnectProviderResponse response = iamClient.createOpenIDConnectProvider(request);
        logger.info("Created IAM OIDC Provider: {}", response.openIDConnectProviderArn());
        return response.openIDConnectProviderArn();
    }

    private static String setupPodRole(IamClient iamClient, String oidcUrl, String oidcProviderArn) {
        // Analytics pods in 'default' namespace using 'analytics-consumer' service account
        String namespace = "default";
        String serviceAccount = "analytics-consumer";

        String trustPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"Federated\": \"" + oidcProviderArn + "\"},"
                + "\"Action\": \"sts:AssumeRoleWithWebIdentity\","
                + "\"Condition\": {"
                + "\"StringEquals\": {"
                + "\"" + oidcUrl + ":sub\": \"system:serviceaccount:" + namespace + ":" + serviceAccount + "\","
                + "\"" + oidcUrl + ":aud\": \"sts.amazonaws.com\""
                + "}"
                + "}"
                + "}]"
                + "}";

        try {
            GetRoleResponse response = iamClient.getRole(GetRoleRequest.builder().roleName(EKS_ANALYTICS_POD_ROLE_NAME).build());
            logger.info("Pod Role '{}' already exists.", EKS_ANALYTICS_POD_ROLE_NAME);
            return response.role().arn();
        } catch (NoSuchEntityException e) {
            logger.info("Creating Pod IAM Role: {}", EKS_ANALYTICS_POD_ROLE_NAME);
            CreateRoleResponse createResponse = iamClient.createRole(CreateRoleRequest.builder()
                    .roleName(EKS_ANALYTICS_POD_ROLE_NAME)
                    .assumeRolePolicyDocument(trustPolicy)
                    .description("Role for EKS pods to access Kinesis")
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build());

            // Attach Kinesis Full permissions (KCL 2.x needs RegisterStreamConsumer etc)
            iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                    .roleName(EKS_ANALYTICS_POD_ROLE_NAME)
                    .policyArn("arn:aws:iam::aws:policy/AmazonKinesisFullAccess")
                    .build());
            
            // Attach DynamoDB access
            iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                    .roleName(EKS_ANALYTICS_POD_ROLE_NAME)
                    .policyArn("arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess")
                    .build());

            // Attach CloudWatch access for KCL metrics
            iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                    .roleName(EKS_ANALYTICS_POD_ROLE_NAME)
                    .policyArn("arn:aws:iam::aws:policy/CloudWatchFullAccess")
                    .build());
            
            logger.info("Pod IAM Role '{}' created and Kinesis/DynamoDB/CloudWatch permissions attached.", EKS_ANALYTICS_POD_ROLE_NAME);
            return createResponse.role().arn();
        }
    }

    private static void setupFrontendInfrastructure(S3Client s3Client, CognitoIdentityClient cognitoClient, 
                                                  IamClient iamClient, StsClient stsClient, KinesisClient kinesisClient) {
        logger.info("Step 6: Setting up Frontend Infrastructure (S3 & Cognito)...");

        // 1. Get Account ID
        GetCallerIdentityResponse callerIdentity = stsClient.getCallerIdentity();
        String accountId = callerIdentity.account();
        String bucketName = WEB_BUCKET_NAME_PREFIX + accountId;

        // 2. Create S3 Bucket
        setupWebBucket(s3Client, bucketName);

        // 3. Setup Cognito Identity Pool and Role
        String identityPoolId = setupCognitoIdentityPool(cognitoClient, iamClient, accountId, kinesisClient);
        
        // 4. Write config for React App
        writeFrontendConfig(identityPoolId, accountId);
        
        logger.info("Frontend Infrastructure Ready!");
        logger.info("Website Bucket: {}", bucketName);
        logger.info("Cognito Identity Pool ID: {}", identityPoolId);
        logger.info("ACTION REQUIRED: The config.json has been written to client/store_front/public/");
    }

    private static void setupWebBucket(S3Client s3Client, String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            logger.info("Web bucket '{}' already exists.", bucketName);
        } catch (NoSuchBucketException e) {
            logger.info("Creating web bucket: {}", bucketName);
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            
            // Enable public access block by default (standard practice)
            // But for website hosting we might need it open or use CloudFront.
            // For this project, we'll keep it simple.
            
            s3Client.putBucketTagging(PutBucketTaggingRequest.builder()
                    .bucket(bucketName)
                    .tagging(Tagging.builder().tagSet(
                            software.amazon.awssdk.services.s3.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build()
                    ).build())
                    .build());
        }
    }

    private static String setupCognitoIdentityPool(CognitoIdentityClient cognitoClient, IamClient iamClient, 
                                                 String accountId, KinesisClient kinesisClient) {
        String identityPoolId;
        
        // Find existing identity pool
        ListIdentityPoolsResponse listPools = cognitoClient.listIdentityPools(ListIdentityPoolsRequest.builder().maxResults(60).build());
        identityPoolId = listPools.identityPools().stream()
                .filter(p -> COGNITO_POOL_NAME.equals(p.identityPoolName()))
                .map(IdentityPoolShortDescription::identityPoolId)
                .findFirst()
                .orElse(null);

        if (identityPoolId == null) {
            logger.info("Creating Cognito Identity Pool: {}", COGNITO_POOL_NAME);
            CreateIdentityPoolResponse response = cognitoClient.createIdentityPool(CreateIdentityPoolRequest.builder()
                    .identityPoolName(COGNITO_POOL_NAME)
                    .allowUnauthenticatedIdentities(true)
                    .build());
            identityPoolId = response.identityPoolId();
        } else {
            logger.info("Cognito Identity Pool already exists: {}", identityPoolId);
        }

        // Create Unauth Role
        String unauthRoleArn = setupUnauthRole(iamClient, identityPoolId, kinesisClient);

        // Map role to identity pool
        cognitoClient.setIdentityPoolRoles(SetIdentityPoolRolesRequest.builder()
                .identityPoolId(identityPoolId)
                .roles(java.util.Map.of("unauthenticated", unauthRoleArn))
                .build());

        return identityPoolId;
    }

    private static String setupUnauthRole(IamClient iamClient, String identityPoolId, KinesisClient kinesisClient) {
        String trustPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"Federated\": \"cognito-identity.amazonaws.com\"},"
                + "\"Action\": \"sts:AssumeRoleWithWebIdentity\","
                + "\"Condition\": {"
                + "\"StringEquals\": {\"cognito-identity.amazonaws.com:aud\": \"" + identityPoolId + "\"},"
                + "\"ForAnyValue:StringLike\": {\"cognito-identity.amazonaws.com:amr\": \"unauthenticated\"}"
                + "}"
                + "}]"
                + "}";

        String roleArn;
        try {
            roleArn = iamClient.getRole(GetRoleRequest.builder().roleName(PUBLIC_PUT_ROLE_NAME).build()).role().arn();
            logger.info("Public Put Role already exists. Updating trust policy...");
            iamClient.updateAssumeRolePolicy(UpdateAssumeRolePolicyRequest.builder()
                    .roleName(PUBLIC_PUT_ROLE_NAME)
                    .policyDocument(trustPolicy)
                    .build());
        } catch (NoSuchEntityException e) {
            logger.info("Creating Public Put Role: {}", PUBLIC_PUT_ROLE_NAME);
            roleArn = iamClient.createRole(CreateRoleRequest.builder()
                    .roleName(PUBLIC_PUT_ROLE_NAME)
                    .assumeRolePolicyDocument(trustPolicy)
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build()).role().arn();
        }

        // Always update the inline policy to ensure permissions are correct
        String streamArn = "arn:aws:kinesis:*:*:stream/" + STREAM_NAME;
        String tableArn = "arn:aws:dynamodb:*:*:table/" + DYNAMODB_TABLE_NAME;
        String policyDocument = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Action\": [\"kinesis:PutRecord\", \"kinesis:PutRecords\", \"kinesis:DescribeStream\"],"
                + "\"Resource\": \"" + streamArn + "\""
                + "}, {"
                + "\"Effect\": \"Allow\","
                + "\"Action\": [\"dynamodb:Query\", \"dynamodb:Scan\"],"
                + "\"Resource\": \"" + tableArn + "\""
                + "}]"
                + "}";

        iamClient.putRolePolicy(PutRolePolicyRequest.builder()
                .roleName(PUBLIC_PUT_ROLE_NAME)
                .policyName("KinesisPublicPutAndDynamoReadPolicy")
                .policyDocument(policyDocument)
                .build());

        return roleArn;
    }

    private static void writeFrontendConfig(String identityPoolId, String accountId) {
        String storeFrontConfigPath = "../../client/store_front/public/config.json";
        String dashboardConfigPath = "../../client/dashboard/public/config.json";
        
        logger.info("Writing frontend configuration to {} and {}...", storeFrontConfigPath, dashboardConfigPath);
        
        String json = String.format("{\n" +
                "  \"identityPoolId\": \"%s\",\n" +
                "  \"region\": \"us-east-1\",\n" +
                "  \"streamName\": \"%s\"\n" +
                "}", identityPoolId, STREAM_NAME);

        try {
            // Write to store_front
            java.io.File sfFile = new java.io.File(storeFrontConfigPath);
            sfFile.getParentFile().mkdirs();
            try (FileWriter writer = new FileWriter(sfFile)) {
                writer.write(json);
            }
            
            // Write to dashboard
            java.io.File dashFile = new java.io.File(dashboardConfigPath);
            dashFile.getParentFile().mkdirs();
            try (FileWriter writer = new FileWriter(dashFile)) {
                writer.write(json);
            }
            
            logger.info("Frontend config.json written successfully to both clients.");
        } catch (IOException e) {
            logger.error("Failed to write frontend config.json: ", e);
        }
    }

    private static void setupECRRepository(EcrClient ecrClient) {
        logger.info("Setting up ECR Repository...");

        try {
            ecrClient.describeRepositories(DescribeRepositoriesRequest.builder()
                    .repositoryNames(ECR_REPOSITORY_NAME)
                    .build());
            logger.info("ECR repository '{}' already exists.", ECR_REPOSITORY_NAME);
        } catch (software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException e) {
            logger.info("Creating new ECR repository: {}", ECR_REPOSITORY_NAME);
            ecrClient.createRepository(CreateRepositoryRequest.builder()
                    .repositoryName(ECR_REPOSITORY_NAME)
                    .tags(software.amazon.awssdk.services.ecr.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build());
            logger.info("ECR repository creation initiated for '{}'.", ECR_REPOSITORY_NAME);
        }
    }

    private static void setupEC2Instances(Ec2Client ec2Client) {
        logger.info("Step 4: Setting up EC2 Instances (Producer & Consumer)...");

        // ── Idempotency check ──────────────────────────────────────
        // Query for running/pending instances with our project tag so re-runs
        // don't keep launching duplicates.
        DescribeInstancesRequest existingCheck = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:" + PROJECT_TAG_KEY).values(PROJECT_TAG_VALUE).build(),
                        Filter.builder().name("tag:Role").values("MetricsProcessor").build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("pending", "running")
                                .build()
                )
                .build();

        DescribeInstancesResponse existingResponse = ec2Client.describeInstances(existingCheck);
        long existingCount = existingResponse.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .count();

        if (existingCount >= 2) {
            logger.info("Found {} existing EC2 instance(s) with project tags. Skipping launch.", existingCount);
            return;
        }

        software.amazon.awssdk.services.ec2.model.DescribeImagesRequest imagesRequest = software.amazon.awssdk.services.ec2.model.DescribeImagesRequest.builder()
                .owners("amazon")
                .filters(
                        Filter.builder().name("name").values("al2023-ami-2023.*-x86_64").build(),
                        Filter.builder().name("state").values("available").build()
                )
                .build();
        
        software.amazon.awssdk.services.ec2.model.DescribeImagesResponse imagesResponse = ec2Client.describeImages(imagesRequest);
        String amiId = imagesResponse.images().stream()
                .sorted((i1, i2) -> i2.creationDate().compareTo(i1.creationDate()))
                .findFirst()
                .map(software.amazon.awssdk.services.ec2.model.Image::imageId)
                .orElseThrow(() -> new RuntimeException("Could not find AL2023 AMI"));

        logger.info("Using latest Amazon Linux 2023 AMI: {}", amiId);

        // ── User Data Script ───────────────────────────────────────
        String userDataScript = "#!/bin/bash\n" +
                "yum update -y\n" +
                "yum install -y python3 python3-pip\n" +
                "pip3 install boto3\n" +
                "cat << 'EOF' > /home/ec2-user/producer.py\n" +
                "import boto3, json, time, random, uuid, datetime\n" +
                "kinesis = boto3.client('kinesis', region_name='us-east-1')\n" +
                "stream_name = 'UserMetricsStream'\n" +
                "event_types = ['mouse_move', 'mouse_click', 'product_add_to_cart']\n" +
                "while True:\n" +
                "  try:\n" +
                "    session_id = str(uuid.uuid4())\n" +
                "    for _ in range(random.randint(5, 20)):\n" +
                "      event = {'timestamp': datetime.datetime.utcnow().isoformat() + 'Z', 'eventType': random.choices(event_types, weights=[80, 15, 5])[0], 'data': {'mock': True}}\n" +
                "      kinesis.put_record(StreamName=stream_name, Data=json.dumps(event), PartitionKey=session_id)\n" +
                "      time.sleep(random.uniform(0.1, 0.5))\n" +
                "    time.sleep(random.uniform(1, 3))\n" +
                "  except Exception as e:\n" +
                "    time.sleep(5)\n" +
                "EOF\n" +
                "chown ec2-user:ec2-user /home/ec2-user/producer.py\n" +
                "nohup python3 /home/ec2-user/producer.py > /home/ec2-user/producer.log 2>&1 &\n";

        String base64UserData = Base64.getEncoder().encodeToString(userDataScript.getBytes());

        // ── Launch instances ───────────────────────────────────────
        int instancesToLaunch = 2 - (int) existingCount;
        logger.info("Launching {} new EC2 instance(s)...", instancesToLaunch);

        RunInstancesRequest runInstancesRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T3_NANO) // Cheapest instance type
                .minCount(instancesToLaunch)
                .maxCount(instancesToLaunch)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name(PRODUCER_PROFILE_NAME).build())
                .userData(base64UserData)
                .tagSpecifications(
                    TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(
                            software.amazon.awssdk.services.ec2.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build(),
                            software.amazon.awssdk.services.ec2.model.Tag.builder().key("Role").value("MetricsProcessor").build()
                        )
                        .build()
                )
                .build();

        // No more swallowed exceptions — let failures propagate to the retry executor.
        RunInstancesResponse response = ec2Client.runInstances(runInstancesRequest);
        List<String> instanceIds = response.instances().stream()
                .map(Instance::instanceId)
                .collect(Collectors.toList());
        
        logger.info("Successfully launched {} EC2 instance(s): {}", instanceIds.size(), instanceIds);
        logger.info("One instance should be configured as the Producer and the other as Consumer.");
    }

    private static void setupProducerIAM(IamClient iamClient) {
        logger.info("Setting up IAM Producer Role and Instance Profile...");
        String trustPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": [{"
                + "\"Effect\": \"Allow\","
                + "\"Principal\": {\"Service\": \"ec2.amazonaws.com\"},"
                + "\"Action\": \"sts:AssumeRole\""
                + "}]"
                + "}";

        try {
            iamClient.getRole(GetRoleRequest.builder().roleName(PRODUCER_ROLE_NAME).build());
            logger.info("Producer Role already exists.");
        } catch (NoSuchEntityException e) {
            iamClient.createRole(CreateRoleRequest.builder()
                    .roleName(PRODUCER_ROLE_NAME)
                    .assumeRolePolicyDocument(trustPolicy)
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build());
            iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
                    .roleName(PRODUCER_ROLE_NAME)
                    .policyArn("arn:aws:iam::aws:policy/AmazonKinesisFullAccess")
                    .build());
            waitForIAMRolePropagation(iamClient, PRODUCER_ROLE_NAME);
        }

        try {
            iamClient.getInstanceProfile(GetInstanceProfileRequest.builder().instanceProfileName(PRODUCER_PROFILE_NAME).build());
            logger.info("Producer Instance Profile already exists.");
        } catch (NoSuchEntityException e) {
            iamClient.createInstanceProfile(CreateInstanceProfileRequest.builder()
                    .instanceProfileName(PRODUCER_PROFILE_NAME)
                    .tags(software.amazon.awssdk.services.iam.model.Tag.builder().key(PROJECT_TAG_KEY).value(PROJECT_TAG_VALUE).build())
                    .build());
            iamClient.addRoleToInstanceProfile(AddRoleToInstanceProfileRequest.builder()
                    .instanceProfileName(PRODUCER_PROFILE_NAME)
                    .roleName(PRODUCER_ROLE_NAME)
                    .build());
            logger.info("Producer Instance Profile created and role added.");
        }
    }
}
