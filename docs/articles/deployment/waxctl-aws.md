As well as facilitating deployment on Kubernetes, Waxctl provides an easy path to deploy dataflows on AWS EC2 instances.

## Installation

To install Waxctl simply download the binary corresponding to your operating system and architecture [here](/downloads/).

## Dataflow Lifecycle

You can manage the lifecycle of your dataflow with Waxctl across the following phases:

- Deployment
- Current Status (Running)
- Restarting
- Deletion

In the following sections we are going to cover each of these phases.

## Available AWS Sub-commands

When using Waxctl with cloud specific resources, you will add the cloud name in front of the sub-command. The `aws` command has the following sub-commands:

- deploy
- list
- stop
- start
- delete

Running `waxctl aws --help` will show further details for these.

```bash
❯ waxctl aws --help
Manage dataflows running on AWS EC2 instances

Usage:
  waxctl aws [command]

Available Commands:
  delete      terminate an EC2 instance created by waxctl
  deploy      create an EC2 instance running a dataflow
  list        list AWS EC2 instances created by waxctl
  start       start an EC2 instance created by waxctl
  stop        stop an EC2 instance created by waxctl

Flags:
  -h, --help   help for aws

Global Flags:
      --debug   enable verbose output

Use "waxctl aws [command] --help" for more information about a command.
```

## Waxctl and AWS CLI

To use Waxctl on AWS you need to have installed and configured the AWS CLI. You can follow the instructions [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

By default, Waxctl is going to use the AWS CLI default region but you can use another one just by setting the `--region` flag in your Waxctl commands.

## Run a Dataflow in an EC2 instance

To deploy a dataflow you just need to run `waxctl aws deploy` passing the path of your python script as an argument. If left unset, Waxctl will use the default name for your dataflow, which is `bytewax`.

In our example we are going to deploy a dataflow called `my-dataflow`:

```bash
❯ waxctl aws deploy /var/bytewax/examples/basic.py --name my-dataflow
Created policy arn:aws:iam::111111111111:policy/Waxctl-EC2-my-dataflow-Policy
Created role Waxctl-EC2-my-dataflow-Role
Created my-dataflow instance with ID i-040f98b9160d2d158 running /var/bytewax/examples/basic.py script
```

In the above example, Waxctl used the default values for all of the flags except for the `name` flag. Waxctl allows you to configure a wide range of characteristics of your dataflow.

As you can see in the output above, Waxctl created an IAM policy and role. That will allow the EC2 instance to store Cloudwatch logs and start sessions through Systems Manager.

We can see the complete list of available flags with the `waxctl aws deploy` help command.
```bash
❯ waxctl aws deploy -h
Deploy a dataflow to a new EC2 instance.

The deploy command expects one argument, which is the path of your python dataflow file.
By default, Waxctl creates a policy and a role that will allow the EC2 instance to store Cloudwatch logs and start sessions through Systems Manager.

Examples:
  # The default command to deploy a dataflow named "bytewax" in a new EC2 instance running my-dataflow.py file.
  waxctl aws deploy my-dataflow.py

  # Deploy a dataflow named "custom" using specific security groups and instance profile
  waxctl aws deploy dataflow.py --name custom \
    --security-groups-ids "sg-006a1re044efb2d23" \
    --principal-arn "arn:aws:iam::1111111111:instance-profile/my-profile"

Usage:
  waxctl aws deploy [PATH] [flags]

Flags:
  -P, --associate-public-ip-address     associate a public IP address to the EC2 instance (default true)
  -m, --detailed-monitoring             specifies whether detailed monitoring is enabled for the EC2 instance
  -e, --extra-tags strings              extra tags to apply to the EC2 instance. The format must be KEY=VALUE
  -h, --help                            help for deploy
  -t, --instance-type string            EC2 instance type to be created (default "t2.micro")
  -k, --key-name string                 name of an existing key pair
  -n, --name string                     name of the EC2 instance to deploy the dataflow (default "bytewax")
  -p, --principal-arn string            principal ARN to assign to the EC2 instance
      --profile string                  AWS cli configuration profile
  -f, --python-file-name string         python script file to run. Only needed when [PATH] is a tar file
      --region string                   AWS region
  -r, --requirements-file-name string   requirements.txt file if needed
      --save-cloud-config               save cloud-config file to disk for troubleshooting purposes
  -S, --security-groups-ids strings     security groups Ids to assign to the EC2 instance
  -s, --subnet-id string                the ID of the subnet to launch the EC2 instance into

Global Flags:
      --debug   enable verbose output
```

We suggest paying special attention to the `requirements-file-name` flag because normally you will want to specify a `requirements.txt` file with the needed libraries to run your dataflow program.

## Default IAM Role

As we mentioned, Waxctl creates an IAM policy and role to allow your EC2 instance to store CloudWatch logs and to start Systems Manager sessions. In case you need to use a custom IAM role, here we show you what are the permissions that the policy created by Waxctl has:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogStreams"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssmmessages:CreateControlChannel",
                "ssmmessages:CreateDataChannel",
                "ssmmessages:OpenControlChannel",
                "ssmmessages:OpenDataChannel",
                "ssm:UpdateInstanceInformation"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetEncryptionConfiguration"
            ],
            "Resource": "*"
        }
    ]
}
```
So, your role must have those permissions to keep both features working.
We recommend attaching to your Role a Customer managed policy having only those permissions and maybe with an explicit name like "Bytewax-Policy" or "Waxctl-Policy".

## Getting Dataflow Information

You can query which dataflows are deployed on EC2 instances in your AWS account using the `waxctl aws list` sub-command. By default the output will be a table with this information:

```bash
❯ waxctl aws ls
Dataflow    Python File Name               VM State Launch Time
my-dataflow /var/bytewax/examples/basic.py running  2022-10-03 13:34:02 +0000 UTC
```

You can use the `--verbose` flag to get more details of each dataflow:

```bash
❯ waxctl aws ls --verbose
[
  {
    "instanceId": "i-040f98b9160d2d158",
    "instanceType": "t2.micro",
    "instanceState": "running",
    "name": "my-dataflow",
    "iamInstanceProfile": "arn:aws:iam::111111111111:instance-profile/Waxctl-EC2-my-dataflow-InstanceProfile",
    "keyName": "",
    "launchTime": "2022-10-03 13:34:02 +0000 UTC",
    "detailedMonitoring": "disabled",
    "privateDnsName": "ip-172-31-3-116.us-west-2.compute.internal",
    "privateIpAddress": "172.31.3.116",
    "pythonFileName": "/var/bytewax/examples/basic.py",
    "publicDnsName": "ec2-18-237-167-156.us-west-2.compute.amazonaws.com",
    "publicIpAddress": "18.237.167.156",
    "securityGroupsIds": [
      "sg-1e2c2c27"
    ],
    "subnetId": "subnet-64840439",
    "tags": [
      {
        "key": "bytewax.io/waxctl-version",
        "value": "0.5.1"
      },
      {
        "key": "Name",
        "value": "my-dataflow"
      },
      {
        "key": "bytewax.io/managed-by",
        "value": "waxctl"
      },
      {
        "key": "bytewax.io/waxctl-managed-instance-profile-name",
        "value": "Waxctl-EC2-my-dataflow-InstanceProfile"
      },
      {
        "key": "bytewax.io/waxctl-managed-policy-arn",
        "value": "arn:aws:iam::111111111111:policy/Waxctl-EC2-my-dataflow-Policy"
      },
      {
        "key": "bytewax.io/waxctl-managed-role-name",
        "value": "Waxctl-EC2-my-dataflow-Role"
      },
      {
        "key": "bytewax.io/waxctl-python-filename",
        "value": "/var/bytewax/examples/basic.py"
      }
    ],
    "consoleDetails": "https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#InstanceDetails:instanceId=i-040f98b9160d2d158",
    "logs": "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fbytewax$252Fsyslog/log-events/my-dataflow$3FfilterPattern$3Dbytewax"
  }
]
```

As you can see, there are a lot of details including links to access instance information and your dataflow logs in the AWS web console.

This is the help text of the `list` command:
```bash
❯ waxctl aws ls --help
List EC2 instances created by waxctl.

Examples:
  # List EC2 instances running dataflows.
  waxctl aws list

  # List EC2 instance named "my dataflow" in region us-east-1.
  waxctl aws list --name "my dataflow" --region us-east-1

Usage:
  waxctl aws list [flags]

Aliases:
  list, ls

Flags:
  -h, --help             help for list
  -n, --name string      name of the EC2 instance to find.
      --profile string   AWS cli configuration profile.
      --region string    AWS region.
  -v, --verbose          return detailed information of the EC2 instances.

Global Flags:
      --debug   enable verbose output
```

## Stopping and Starting a Dataflow

In case you need to pause your dataflow, there are two commands to manage that: `stop` and `start`. These control the EC2 instance state where your dataflow is running.

Following the example, you can stop the dataflow EC2 instance with this command:

```bash
❯ waxctl aws stop --name my-dataflow
EC2 instance my-dataflow with ID i-040f98b9160d2d158 stopped.
```

So, if you run the `list` command, you will see something like this:

```bash
❯ waxctl aws ls
Dataflow    Python File Name               VM State Launch Time
my-dataflow /var/bytewax/examples/basic.py stopping 2022-10-03 13:34:02 +0000 UTC
```

And a few seconds later, the `list` command is going to show the state `stopped`:

```bash
❯ waxctl aws ls
Dataflow    Python File Name               VM State Launch Time
my-dataflow /var/bytewax/examples/basic.py stopped  2022-10-03 13:34:02 +0000 UTC
```

You can use the `start` command to start the EC2 instance again:

```bash
❯ waxctl aws start --name my-dataflow
EC2 instance my-dataflow with ID i-040f98b9160d2d158 started.
```

You can change any of the flags of your dataflow.

## Removing a Dataflow

To terminate the EC2 instance where your dataflow is running you need to run `waxctl aws delete` while passing the name of the dataflow as a parameter.

To run a dry-run delete of our dataflow example we can run this:
```bash
❯ waxctl aws delete --name my-dataflow
EC2 instance my-dataflow with ID i-040f98b9160d2d158 found in us-west-2 region.

--yes flag is required to terminate it.
```

And if we want to actually delete the dataflow we must add the `--yes` flag:
```bash
❯ waxctl aws rm --name my-dataflow --yes
Role Waxctl-EC2-my-dataflow-Role deleted.
Policy arn:aws:iam::111111111111:policy/Waxctl-EC2-my-dataflow-Policy deleted.
EC2 instance my-dataflow with ID i-040f98b9160d2d158 has been terminated.
```

Note that we used `rm` in the last command, which is an alias of `delete`. Many of the Waxctl sub-commands have an alias and you can see them in the help.

## How it works internally

As you can imagine, Waxctl uses the AWS API to manage EC2 instances, IAM policies, and roles.

The operating system of EC2 instances created by Waxctl is Ubuntu 20.04 LTS.

Waxctl relies on [Cloud-init](https://cloudinit.readthedocs.io/en/latest/), a standard multi-distribution method for cross-platform cloud instance initialization. Using Cloud-init, Waxctl configures a Linux service which is going to run `pip install -r /home/ubuntu/bytewax/requirements.txt` and after that run your python dataflow program.

As we mentioned before, you can specify your own `requirements.txt` file using the `--requirements-file-name` flag. If you don't, Waxctl is going to put only `bytewax` as a requirement.

Besides setting the service that runs your dataflow, Waxctl configures settings to push the syslog logs to CloudWatch. With that enabled, you can see your dataflow stdout and stderr in CloudWatch.

## Troubleshooting

You have two ways to see what's going on with your dataflow program: viewing logs and connect to the EC2 instance.

### Logs

Since Waxctl runs your dataflow program a Linux service and all syslog logs are sent to CloudWatch, you can see your dataflow logs directly in CloudWatch.

When you run `waxctl aws ls --verbose` you get a link to CloudWatch Logs Viewer in AWS Web Console filtering by your EC2 instance and your dataflow. Like this:

```
...
    "logs": "https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Fbytewax$252Fsyslog/log-events/my-dataflow$3FfilterPattern$3Dbytewax"
...
```

### Connecting to the EC2 Instance

By default Waxctl will create an IAM Policy and a IAM Role to allow you to use AWS Systems Manager to connect to the EC2 instance. To do that, you need to know the instance ID (you can get it running `waxctl aws ls --verbose`) and then run this:

```bash
aws ssm start-session --target i-0a04d5e18c1df4c90
```

You may want to check:

- /home/ubuntu/bytewax - where your requirements and python script are copied.
- `systemctl status bytewax-dataflow.service` - Linux service that runs your dataflow.
- `df -H /` - File system information.
- `top` - Processes information.

You can install any profiling or debugging tool and use it. Also you could modify your script and restart the `bytewax-dataflow.service` running:

```bash
systemctl restart bytewax-dataflow.service
```

## A Production-like Example

In case your dataflow needs access to other AWS managed services, like MKS, you will probably want to use your own Security Group and IAM configuration.

In a production environment, the EC2 instance should be running in a specific Subnet, commonly a private one so we are going to instruct Waxctl to not associate a public IP address and to create the EC2 in a concrete Subnet.

So, this is an example Waxctl command for the described scenario:

```bash
waxctl aws deploy my-dataflow.py \
  --name=production-dataflow \
  --requirements-file-name=requirements.txt \
  --instance-type=m5.xlarge \
  --security-groups-ids=sg-0eae80fc9370624f7 \
  --principal-arn=arn:aws:iam::111111111111:instance-profile/production-profile \
  --subnet-id=subnet-03ec94a8d7d22d8e9 \
  --debug
```

The output of that deploy will be like this:

```
2022/10/05 10:31:05 Analylics - information to send:
 {"waxctl_version":"0.5.1","platform":"amd64","os":"linux","command":"aws","subcommand":"deploy"}
2022/10/05 10:31:05 Analylics - duration: 942.50645ms
2022/10/05 10:31:05 Validating parameters...
2022/10/05 10:31:05 Getting AWS region us-west-2 from default configuration
2022/10/05 10:31:05 Getting AWS CLI configuration...
2022/10/05 10:31:07 Creating cloud-init config file...
2022/10/05 10:31:08 Selected AMI - Name: ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20220924 - ImageId: ami-07eeacb3005b9beae
Created production-dataflow instance with ID i-0409c71c2cc1b249f running my-dataflow.py script
```
