
# S3DropBucket 


This project contains source code and supporting files for the serverless application S3DropBucket


Update the application by committing and pushing changes to the AWS CodeCommit repository.

This project includes the following files and folders:

- src - Code for the application's Lambda function.
- events - Invocation events that you can use to invoke the function.
- \_\_tests__ - Unit tests for the application code.
- template.yml - A SAM template that defines the application's AWS resources.
- buildspec.yml -  A build specification file that tells AWS CodeBuild how to create a deployment package for the function.

This Lambda monorepo application includes two AWS CloudFormation stacks. 
    The first stack creates the pipeline that builds and deploys the application.
    The second stack contains the application's resources, including Lambda functions, and an Amazon S3 bucket. 
        These resources are defined in the `template.yml` file in this project. 
        Update the template to add AWS resources through the same deployment process that updates the application code (vscode, etc...). 
        You can view resources in the **Resources** section of the application overview in the Lambda console.

For a full list of possible operations, see the [AWS Lambda Applications documentation](https://docs.aws.amazon.com/lambda/latest/dg/deploying-lambda-apps.html).

    ## Try the application out
    
    1. Go to the Lambda console.
    2. Select **Applications** and select the one you created.
    3. Select **S3Bucket** in **Resources** table, which will redirect you to the S3 console.
    4. Click on the **Upload** tab, then click **Add files** to choose a file that ends in `.json` to add to the bucket. You can use the example file in the repository, found at `__tests__/s3-example/log.json`.
    5. Go back to the Lambda console, and find your application again.
    6. Select **s3JsonLoggerFunction** in the **Resources** table.
    7. Select the **Monitoring** tab, then click **View logs in CloudWatch**, which will take you to the CloudWatch Logs console.
    8. Click on the latest log stream entry, and you will find the content of the file you added to the S3 bucket in the log stream.


## Add a resource to the application

The application template uses the AWS Serverless Application Model (AWS SAM) to define application resources. AWS SAM is an extension of AWS CloudFormation with a simpler syntax for configuring common serverless application resources, such as functions, triggers, and APIs. For resources that aren't included in the [AWS SAM specification](https://github.com/awslabs/serverless-application-model/blob/master/versions/2016-10-31.md), you can use the standard [AWS CloudFormation resource types](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-template-resource-type-ref.html).

Update `template.yml` to add a dead-letter queue to your application. In the **Resources** section, add a resource named **MyQueue** with the type **AWS::SQS::Queue**.

```
Resources:
  MyQueue:
    Type: AWS::SQS::Queue
```

The dead-letter queue is a location for Lambda to send events that could not be processed. It's only used if you invoke your function asynchronously, but it's useful here to show how you can modify your application's resources and function configuration.

Commit the change and push.

```bash
my-application$ git commit -am "Add dead-letter queue."
my-application$ git push
```

**To see how the pipeline processes and deploys the change**

1. Open the [**Applications**](https://console.aws.amazon.com/lambda/home#/applications) page.
1. Choose your application.
1. Choose **Deployments**.

When the deployment completes, view the application resources on the **Overview** tab to see the new resource.

## Update the permissions boundary

The sample application applies a **permissions boundary** to its function's execution role. The permissions boundary limits the permissions that you can add to the function's role. Without the boundary, users with write access to the project repository could modify the project template to give the function permission to access resources and services outside of the scope of the sample application.

In order for the function to use the queue that you added in the previous step, you must extend the permissions boundary. The Lambda console detects resources that aren't in the permissions boundary and provides an updated policy that you can use to update it.

**To update the application's permissions boundary**

1. Open the [**Applications**](https://console.aws.amazon.com/lambda/home#/applications) page.
1. Choose your application.
1. Choose **Edit permissions boundary**.
1. Follow the instructions shown to update the boundary to allow access to the new queue.

## Update the function configuration

Now you can grant the function permission to access the queue and configure the dead-letter queue setting.

In the function's properties in `template.yml`, add the **DeadLetterQueue** configuration. Under Policies, add **SQSSendMessagePolicy**. **SQSSendMessagePolicy** is a [policy template](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-policy-templates.html) that grants the function permission to send messages to a queue.

```
Resources:
  MyQueue:
    Type: AWS::SQS::Queue
  s3JsonLoggerFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: ./
      Handler: src/handlers/s3-json-logger.s3JsonLoggerHandler
      Runtime: nodejs18.x
      Description: A Lambda function that logs a json file sent to S3 bucket.
      MemorySize: 128
      Timeout: 60
      DeadLetterQueue:
        Type: SQS
        TargetArn: !GetAtt MyQueue.Arn
      Policies:
        - SQSSendMessagePolicy:
            QueueName: !GetAtt MyQueue.QueueName
        - S3ReadPolicy:
            BucketName: !Sub aws-${AWS::Region}-${AWS::AccountId}-${AppId}-simpleappbucket
      Events:
        simpleappbucketEvent:
          Type: S3
          Properties:
            Bucket: !Ref simpleappbucket
            Events: s3:ObjectCreated:*
            Filter:
              S3Key:
                Rules:
                  - Name: suffix
                    Value: ".json"
```

Commit and push the change. When the deployment completes, view the function in the console to see the updated configuration that specifies the dead-letter queue.

## Build and test locally

The AWS SAM command line interface (CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. It uses Docker to run your functions in an Amazon Linux environment that matches Lambda. It can also emulate your application's build environment and API.

If you prefer to use an integrated development environment (IDE) to build and test your application, you can use the AWS Toolkit.
The AWS Toolkit is an open-source plugin for popular IDEs that uses the AWS SAM CLI to build and deploy serverless applications on AWS. The AWS Toolkit also adds step-through debugging for Lambda function code.

To get started, see the following:

* [PyCharm](https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/welcome.html)
* [IntelliJ](https://docs.aws.amazon.com/toolkit-for-jetbrains/latest/userguide/welcome.html)
* [VS Code](https://docs.aws.amazon.com/toolkit-for-vscode/latest/userguide/welcome.html)
* [Visual Studio](https://docs.aws.amazon.com/toolkit-for-visual-studio/latest/user-guide/welcome.html)

To use the AWS SAM CLI with this sample, you need the following tools:

* AWS CLI - [Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) and [configure it with your AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
* AWS SAM CLI - [Install the AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community).

Build your application with the `sam build` command.

```bash
my-application$ sam build -m package.json
```

The AWS SAM CLI installs dependencies that are defined in `package.json`, creates a deployment package, and saves its contents in the `.aws-sam/build` folder.

Test a single function by invoking it directly with a test event. An event is a JSON document that represents the input that the function receives from the event source. Test events are included in the `events` folder in this project.

Run functions locally and invoke them with the `sam local invoke` command.

```bash
my-application$ sam local invoke s3JsonLoggerFunction --event events/event-s3.json
```

## Unit tests

Requirements:

* Node.js - [Install Node.js 18.x](https://nodejs.org/en/), including the npm package management tool.

Tests are defined in the \_\_tests__ folder in this project. Use `npm` to install the [Jest test framework](https://jestjs.io/) and run unit tests.

```bash
my-application$ npm install
my-application$ npm run test
```

## Resources

For an introduction to the AWS SAM specification, the AWS SAM CLI, and serverless application concepts, see the [AWS SAM Developer Guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).

Next, you can use the AWS Serverless Application Repository to deploy ready-to-use apps that go beyond Hello World samples and learn how authors developed their applications. For more information, see the [AWS Serverless Application Repository main page](https://aws.amazon.com/serverless/serverlessrepo/) and the [AWS Serverless Application Repository Developer Guide](https://docs.aws.amazon.com/serverlessrepo/latest/devguide/what-is-serverlessrepo.html).
