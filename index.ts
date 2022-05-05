import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

///////////////////////////////////////////////
// Two IAM policy options for Lambda functions
///////////////////////////////////////////////
type PolicyType = "dynamodb" | "sagemaker" ;

////////////////////////////////////////////////////////////////
// ComponentResource class to encapsulate S3 and IAM for Lambda 
////////////////////////////////////////////////////////////////
class BucketComponent extends pulumi.ComponentResource {
    public bucket: aws.s3.Bucket;
    private iamPolicy: aws.iam.RolePolicy;
    public iamRole: aws.iam.Role; 
    private allowBucket: aws.lambda.Permission;
    private bucketNotification: aws.s3.BucketNotification;
    private lambdaFunction: aws.lambda.Function;
    private s3Object: aws.s3.BucketObject;
    
    /////////////////////////////////////////////////
    // Get the IAM policy based on the provided type
    /////////////////////////////////////////////////
    private policies: { [K in PolicyType]: aws.iam.PolicyStatement } = {
        dynamodb: {
            Sid: "dynamodbTable",
            Effect: "Allow",
            Resource: "*",
            Action: [
                "dynamodb:BatchGet*",
                "dynamodb:DescribeStream",
                "dynamodb:DescribeTable",
                "dynamodb:Get*",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchWrite*",
                "dynamodb:CreateTable",
                "dynamodb:Delete*",
                "dynamodb:Update*",
                "dynamodb:PutItem"
            ],
        },
        sagemaker: {
            Sid: "sagemakerRuntime",
            Effect: "Allow",
            Resource: "*",
            Action: [
                "sagemaker:CreateDataQualityJobDefinition"
            ],
        }
    };

    //////////////////////////////////////////////////////////////////////
    // Return the IAM policy which includes logging to CloudWatch, always
    //////////////////////////////////////////////////////////////////////
    private getIamPolicy(policyType: PolicyType): aws.iam.PolicyDocument {
        return {
            Version: "2012-10-17",
            Statement: [{
                ...this.policies[policyType]
            },
            {
                Effect: "Allow",
                Action: "logs:CreateLogGroup",
                Resource: "arn:aws:logs:*:*:*"
            },
            {
                Effect: "Allow",
                Action: [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                Resource: [
                    "arn:aws:logs:*:*:log-group:/aws/lambda/*:*"
                ]
            }
            ],
        }
    };

    ////////////////////////////////////////////////////////
    // Create S3 bucket, IAM role, and IAM policy resources
    ////////////////////////////////////////////////////////
    constructor(name: string, args: { policyType: PolicyType , lambdaCode: string , environmentVariables: any }, opts?: pulumi.ComponentResourceOptions) {

        // By calling super(), we ensure any instantiation of this class
        // inherits from the ComponentResource class so we don't have to
        // declare all the same things all over again.
        super("pkg:index:BucketComponent", name, args, opts);

        this.bucket = new aws.s3.Bucket(name + "-S3Bucket", {}, { parent: this });

        this.iamRole = new aws.iam.Role(name + "-LambdaIamRole", {
            assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({
                Service: "lambda.amazonaws.com"})
        }, { parent: this })
        
        this.iamPolicy = new aws.iam.RolePolicy(name + "-LambdaIamPolicy", {
            role: this.iamRole.id,
            policy: this.getIamPolicy(args.policyType),
        }, { parent: this });
        
        this.lambdaFunction = new aws.lambda.Function(name + "-LambdaFunction", {
            role: this.iamRole.arn,
            handler: "lambda_function.lambda_handler",
            runtime: "python3.9",
            environment: {
                variables: args.environmentVariables,
            },
            code: new pulumi.asset.AssetArchive({
                "lambda_function.py": new pulumi.asset.StringAsset(
                args.lambdaCode,
                ),
            }),
        }, { parent: this })
        
        this.allowBucket = new aws.lambda.Permission(name + "-LambdaFunctionResourcePolicy", {
            action: "lambda:InvokeFunction",
            "function": this.lambdaFunction.arn,
            principal: "s3.amazonaws.com",
            sourceArn: this.bucket.arn,
        }, { parent: this });
        
        this.bucketNotification = new aws.s3.BucketNotification(name + "-S3BucketNotification", {
            bucket: this.bucket.id,
            lambdaFunctions: [{
                lambdaFunctionArn: this.lambdaFunction.arn,
                events: ["s3:ObjectCreated:*"],
            }],
        }, {
            dependsOn: [this.allowBucket], parent: this
        });
        
        this.s3Object = new aws.s3.BucketObject(name + "-S3BucketObject", {
            key: "index.ts",
            bucket: this.bucket.id,
            source: new pulumi.asset.FileAsset("index.ts")
        }, { parent: this , dependsOn: this.bucketNotification });
        
        ///////////////////////
        // Return output to UI 
        ///////////////////////
        this.registerOutputs({
            bucketName: this.bucket.id,
            iamPolicy: this.iamPolicy.id,
            iamRole: this.iamRole.id,
            allowBucket: this.allowBucket.sourceArn,
            bucketNotification: this.bucketNotification.id,
            lambdaFunction: this.lambdaFunction.id,
            s3Object: this.s3Object.key,
        });
    }
}

//////////////////////////////////////////////////////////////////////
// The Lambda code needs a DynamoDB table with a unique configuration 
//////////////////////////////////////////////////////////////////////
const dynamodb = new aws.dynamodb.Table("s3EventsToDynamoDb-DynamoDBTable", {
    attributes: [
        {
            name: "x-amz-request-id",
            type: "S",
        },
        {
            name: "key",
            type: "S",
        },
        {
            name: "eventTime",
            type: "S",
        },
    ],
    billingMode: "PAY_PER_REQUEST",
    globalSecondaryIndexes: [{
        hashKey: "key",
        name: "key-eventTime-index",
        nonKeyAttributes: ["x-amz-request-id"],
        projectionType: "INCLUDE",
        rangeKey: "eventTime",
    }],
    hashKey: "x-amz-request-id",
    tags: {
        Environment: "dev",
        Name: "obj-index-table",
    },
});

/////////////////////////////////////////////////////////////////
// Create bucket with DynamoDB IAM policy type for this instance 
/////////////////////////////////////////////////////////////////
const bucket = new BucketComponent("s3EventsToDynamoDb", {
    policyType: "dynamodb",
    environmentVariables: {table: dynamodb.name,},
    lambdaCode: `import boto3
import os 

dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    requestId = event['Records'][0]['responseElements']['x-amz-request-id']
    key = event['Records'][0]['s3']['object']['key']
    eventTime = event['Records'][0]['eventTime']
    table = os.environ['table']
    try:
        response = dynamodb.put_item(TableName=table, Item={'x-amz-request-id':{'S':requestId},'key':{'S':key},'eventTime':{'S':eventTime}})
        return response
    except Exception as e:
        print(e)
        print('Error putting S3 request id {} with key {} at {} into DynamoDB table {}. It may have sent twice. Make sure this item exists in your table.'.format(x-amz-requst-id, key, eventTime, table))
        raise e
    return {
        'statusCode': 200, 
        'body': response 
    }`
});

/////////////////////////////////////////////////////////////////
// Reusing the ComponentResource - annnd May the 4th be with you 
/////////////////////////////////////////////////////////////////
const bucket2 = new BucketComponent("theOtherOne", {
    policyType: "sagemaker",
    environmentVariables: {foo: "bar",},
    lambdaCode: `def lambda_handler(event, context):
    print("hello there")
    print("GeNeRaL kEnObI")`
});

///////////////////////////////////
// Export resource names to the UI 
///////////////////////////////////
export const bucketName = bucket.bucket.id;
export const bucket2Name = bucket2.bucket.id;
export const dynamoDbTableName = dynamodb.name;