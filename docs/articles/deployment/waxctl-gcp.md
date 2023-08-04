Waxctl allows you to deploy dataflows on Google Cloud Platform VM instances.

## Installation

To install Waxctl simply download the binary corresponding to your operating system and architecture [here](/downloads/).

## Dataflow Lifecycle

You can manage the lifecycle of your dataflow with Waxctl across the following phases:

- Deployment
- Current Status (Running)
- Restarting
- Deletion

In the following sections we are going to cover each of these phases.

## Available GCP Sub-commands

When using Waxctl with cloud specific resources, you will add the cloud name in front of the sub-command. The `gcp` command has the following sub-commands:

- deploy
- list
- stop
- start
- delete

Running `waxctl gcp --help` will show further details for these.

```bash
❯ waxctl gcp --help
Manage dataflows running on GCP VM instances.

Prerequisites:
- Enable Compute Engine API
- Enable Cloud Resource Manager API
- Set up Application Default Credentials running: gcloud auth application-default login

Usage:
  waxctl gcp [flags]
  waxctl gcp [command]

Available Commands:
  delete      delete a VM instance created by waxctl
  deploy      create VM instance running a dataflow
  list        list VM instances created by waxctl
  start       start a VM instance created by waxctl
  stop        stop a VM instance created by waxctl

Flags:
  -h, --help   help for gcp

Global Flags:
      --debug   enable verbose output

Use "waxctl gcp [command] --help" for more information about a command.
```

## Waxctl and Google Cloud CLI

To use Waxctl on GCP, first, you need to have installed and configured the gcloud CLI. You can follow the instructions [here](https://cloud.google.com/sdk/docs/install-sdk).

After that, you need to set up Application Default Credentials running:

```bash
gcloud auth application-default login
```

Finally, you must check if these APIs are enabled:

```
NAME                                 TITLE
cloudresourcemanager.googleapis.com  Cloud Resource Manager API
compute.googleapis.com               Compute Engine API
```

To know that, you can run this:

```bash
gcloud services list --enabled
```

If you don't have any of them enabled, you will need to run this:

```bash
gcloud services enable SERVICE_NAME
```

For example, to enable Compute Engine API you will run:

```bash
gcloud services enable compute.googleapis.com
```

By default, Waxctl is going to use the project configured in the application default credentials that you defined but you can use another one just by setting the `--project` flag in your Waxctl commands.

## Run a Dataflow in a VM instance

To deploy a dataflow you just need to run `waxctl gcp deploy` passing the path of your python script as an argument and the desired GCP zone using the `--zone` flag. If left unset, Waxctl will use the default name for your dataflow, which is `bytewax`.

In our example we are going to deploy a dataflow called `my-dataflow`:

```bash
waxctl gcp deploy /var/bytewax/examples/basic.py --name my-dataflow --zone us-central1-a
Created custom role projects/my-project/roles/my_dataflow
Created service account my-dataflow@my-project.iam.gserviceaccount.com
Created policy binding service account my-dataflow@my-project.iam.gserviceaccount.com with role projects/my-project/roles/my_dataflow
Created my-dataflow instance in zone us-central1-a of project my-project running /var/bytewax/examples/basic.py script.
```

In the above example, Waxctl used the default values for all of the flags except for `name` and `zone`. Waxctl allows you to configure a wide range of characteristics of your dataflow.

As you can see in the output above, Waxctl created an IAM role and service account. That will allow the VM instance to store StackDriver logs.

We can see the complete list of available flags with the `waxctl gcp deploy` help command.

```bash
❯ waxctl gcp deploy --help
Deploy a dataflow to a new GCP VM instance.

The deploy command expects one argument, which is the path of your python dataflow file.
By default, Waxctl creates a role and a service account that will allow the VM instance to store StackDriver logs.

Examples:
  # The default command to deploy a dataflow named "bytewax" in a new VM instance running my-dataflow.py file.
  waxctl gcp deploy my-dataflow.py --zone us-central1-a

  # Deploy a dataflow named "custom" using specific network tags and service account
  waxctl gcp deploy dataflow.py --name custom --zone us-central1-a \
    --tags "dataflow,production" \
    --service-account-email "dataflows-service-account@my-project.iam.gserviceaccount.com"

Usage:
  waxctl gcp deploy [PATH] [flags]

Flags:
  -P, --associate-public-ip-address     associate a public IP address to the VM instance (If it's false, need to set up a Cloud NAT in the VPC because VM must access the internet) (default true)
  -e, --extra-labels strings            extra labels to apply to the VM instance. The format must be key=value (both key and value can only contain lowercase letters, numeric characters, underscores and dashes.)
  -h, --help                            help for deploy
  -t, --machine-type string             VM instance machine type to be created (default "n1-standard-1")
  -n, --name string                     name of the VM instance to deploy the dataflow (must consist of lowercase letters (a-z), numbers, and hyphens) (default "bytewax")
  -p, --project-id string               GCP project ID
  -f, --python-file-name string         python script file to run. Only needed when [PATH] is a tar file
  -r, --requirements-file-name string   requirements.txt file if needed
      --save-cloud-config               save cloud-config file to disk for troubleshooting purposes
  -S, --service-account-email string    service account to assign to the VM instance
  -s, --subnetwork-link string          the link of the subnetwork to launch the VM instance into (The format must be: regions/<REGION>/subnetworks/<SUBNETWORK_NAME>)
  -T, --tags strings                    network tags to assing to the VM instance (The format must be: tag1,tag2,tag3)
  -z, --zone string                     GCP zone

Global Flags:
      --debug   enable verbose output
```

We suggest paying special attention to the `requirements-file-name` flag because normally you will want to specify a `requirements.txt` file with the needed libraries to run your dataflow program.

## Default IAM Service Account

As we mentioned, Waxctl creates an IAM service account to allow your VM instance to store StackDriver logs. In case you need to use a custom IAM service account, the only permission that you need to assing to your service account is `logging.logEntries.create`

We recommend to create an exclusive role having only that permission and maybe with an explicit name like "Bytewax-Role" or "Waxctl-Role". Then, you need to bind that role to your custom service account.

## Getting Dataflow Information

You can query which dataflows are deployed on VM instances in your GCP account using the `waxctl gcp list` sub-command. By default the output will be a table with this information:

```bash
❯ waxctl gcp ls
Dataflow    Python File Name VM State Zone          Launch Time (UTC)
my-dataflow basic-py         RUNNING  us-central1-a 2022-10-13T07:01:34.833-07:00
```

You can use the `--verbose` flag to get more details of each dataflow:

```bash
❯ waxctl gcp ls --verbose
[
  {
    "instanceId": 4460149001200968897,
    "instanceType": "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/machineTypes/n1-standard-1",
    "instanceState": "RUNNING",
    "name": "my-dataflow",
    "serviceAccounts": [
      "my-dataflow@my-project.iam.gserviceaccount.com"
    ],
    "launchTime": "2022-10-13T07:01:34.833-07:00",
    "privateIpAddress": "10.128.0.38",
    "pythonFileName": "basic-py",
    "publicIpAddress": "34.135.142.229",
    "networkTags": [],
    "subnetworkLink": "https://www.googleapis.com/compute/v1/projects/my-project/regions/us-central1/subnetworks/default",
    "zone": "https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a",
    "labels": [
      {
        "key": "bytewax_io-managed_by",
        "value": "waxctl"
      },
      {
        "key": "bytewax_io-waxctl_dataflow_name",
        "value": "my-dataflow"
      },
      {
        "key": "bytewax_io-waxctl_managed_role_name",
        "value": "my_dataflow"
      },
      {
        "key": "bytewax_io-waxctl_managed_service_account_id",
        "value": "110115571217054613003"
      },
      {
        "key": "bytewax_io-waxctl_python_filename",
        "value": "basic-py"
      },
      {
        "key": "bytewax_io-waxctl_version",
        "value": "0-5-1"
      }
    ],
    "consoleDetails": "https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/my-dataflow?project=my-project",
    "logs": "https://console.cloud.google.com/logs/query;query=%2528resource.type%3D%22gce_instance%22%0Aresource.labels.instance_id%3D%224460149001200968897%22%2529%0Arun-bytewax-dataflow.sh%0A;timeRange=PT10H?project=my-project"
  }
]
```

As you can see, there are a lot of details including links to access instance information and your dataflow logs in the GCP web console.

This is the help text of the `list` command:
```bash
❯ waxctl gcp ls --help
List VM instances created by waxctl.

Examples:
  # List VM instances running dataflows in us-central1-a zone.
  waxctl gcp list --zone us-central1-a

  # List VM instance named "my dataflow" in zone us-west1-a.
  waxctl gcp list --name "my dataflow" --zone us-west1-a

Usage:
  waxctl gcp list [flags]

Aliases:
  list, ls

Flags:
  -h, --help               help for list
  -n, --name string        name of the VM instance to find.
  -p, --projectId string   GCP project Id.
  -v, --verbose            return detailed information of the VM instances.
  -z, --zone string        GCP zone.

Global Flags:
      --debug   enable verbose output
```

## Stopping and Starting a Dataflow

In case you need to pause your dataflow, there are two commands to manage that: `stop` and `start`. These control the VM instance state where your dataflow is running.

Following the example, you can stop the dataflow VM instance with this command:

```bash
❯ waxctl gcp stop --name my-dataflow --zone us-central1-a
Stopping VM instance my-dataflow in us-central1-a zone in project my-project...
VM instance my-dataflow with ID 4460149001200968897 stopped.
```

So, if you run the `list` command, you will see something like this:

```bash
❯ waxctl gcp ls
Dataflow    Python File Name VM State   Zone          Launch Time (UTC)
my-dataflow basic-py         TERMINATED us-central1-a 2022-10-13T07:01:34.833-07:00
```

You can use the `start` command to start the VM instance again:

```bash
❯ waxctl gcp start --name my-dataflow --zone us-central1-a
Starting VM instance my-dataflow in us-central1-a zone in project my-project...
VM instance my-dataflow with ID 4460149001200968897 started.
```

You can change any of the flags of your dataflow.

## Removing a Dataflow

To terminate the VM instance where your dataflow is running you need to run `waxctl gcp delete` while passing the name of the dataflow as a parameter.

To run a dry-run delete of our dataflow example we can run this:
```bash
❯ waxctl gcp delete --name my-dataflow --zone us-central1-a
VM instance my-dataflow with ID 4460149001200968897 found in us-central1-a zone.

 --yes flag is required to delete it.
```

And if we want to actually delete the dataflow we must add the `--yes` flag:
```bash
❯ waxctl gcp delete --name my-dataflow --zone us-central1-a --yes
Deleting my-dataflow instance from project my-project. It could take some minutes, please wait...
Role my_dataflow deleted.
Service Account my-dataflow@my-project.iam.gserviceaccount.com deleted.
VM instance my-dataflow with ID 4460149001200968897 has been deleted from project my-project.
```

Note that we used `rm` in the last command, which is an alias of `delete`. Many of the Waxctl sub-commands have an alias and you can see them in the help.

## How it works internally

As you can imagine, Waxctl uses the GCP API to manage VM instances, IAM roles, and service accounts.

The operating system of EC2 instances created by Waxctl is Ubuntu 20.04 LTS.

Waxctl relies on [Cloud-init](https://cloudinit.readthedocs.io/en/latest/), a standard multi-distribution method for cross-platform cloud instance initialization. Using Cloud-init, Waxctl configures a Linux service which is going to run `pip install -r /home/ubuntu/bytewax/requirements.txt` and after that run your python dataflow program.

As we mentioned before, you can specify your own `requirements.txt` file using the `--requirements-file-name` flag. If you don't, Waxctl is going to put only `bytewax` as a requirement.

Besides setting the service that runs your dataflow, Waxctl configures settings to push the syslog logs to StackDriver. With that enabled, you can see your dataflow stdout and stderr in Log Explorer.

## Troubleshooting

You have two ways to see what's going on with your dataflow program: viewing logs and connect to the VM instance.

### Logs

Since Waxctl runs your dataflow program a Linux service and all syslog logs are sent to StackDriver, you can see your dataflow logs directly in Log Explorer.

When you run `waxctl gcp ls --verbose` you get a link to Log Explorer in GCP web console filtering by your VM instance and your dataflow. Like this:

```
...
    "logs": "https://console.cloud.google.com/logs/query;query=%2528resource.type%3D%22gce_instance%22%0Aresource.labels.instance_id%3D%22760540438160841495%22%2529%0Arun-bytewax-dataflow.sh%0A;timeRange=PT10H?project=my-project"
...
```

### Connecting to the VM Instance

If your GCP network configuration allows your VM instance to receive traffic in port 22, you can run this command to connect to the instance:

```bash
❯ gcloud compute ssh \
  --zone "us-central1-a" "ubuntu@my-dataflow" \
  --project "my-project"
```

About port 22, Waxctl by default uses the default subnet of the default VPC (also called Network) in the zone that you specified in your project. Normally that Network has a firewall rule `default-allow-ssh` which set what its name says: allows ingress traffic in port 22 from everywhere.

So, if you want to use `gcloud compute ssh` to connect to your VM instance and you are going to use a custom subnet, you need to have a similar firewall configuration in that network (maybe without having `0.0.0.0\0` as source CIDR).

Once you are connected to your instance, you may want to check:

- /home/ubuntu/bytewax - where your requirements and python script are copied.
- `systemctl status bytewax-dataflow.service` - Linux service that runs your dataflow.
- `df -H /` - File system information.
- `top` - Processes information.

You can install any profiling or debugging tool and use it. Also you could modify your script and restart the `bytewax-dataflow.service` running:

```bash
systemctl restart bytewax-dataflow.service
```

## A Production-like Example

In case your dataflow needs access to other GCP-managed services, like BigQuery, you will probably want to use your network tags, IAM configuration, and custom subnet.

In a production environment, the VM instance should be running in a specific subnet, commonly a private one so we are going to instruct Waxctl to not associate a public IP address and to create the VM instance in a concrete subnet. Take note that in that scenario, you will need to set up a Cloud NAT in the VPC of that subnet because the VM needs to access the internet.

So, this is an example Waxctl command for the described scenario:

```bash
❯ waxctl gcp deploy --associate-public-ip-address=false \
  /var/bytewax/examples/production.py \
  --name "production-dataflow" \
  --zone us-central1-a \
  --requirements-file-name /var/bytewax/examples/requirements.txt \
  --extra-labels environment=production \
  --service-account-email my-service-account@my-project.iam.gserviceaccount.com\
  --tags dataflow,production,web \
  --subnetwork-link projects/my-project/regions/us-central1/subnetworks/mysubnet \
  --debug
```

The output of that deploy will be like this:

```
2022/10/13 15:19:44 Analylics - information to send:
{"waxctl_version":"0.5.1","platform":"amd64","os":"linux","command":"gcp","subcommand":"deploy"}
2022/10/13 15:19:45 Analylics - duration: 861.582911ms
2022/10/13 15:19:45 replace result: production-dataflow-2
2022/10/13 15:19:45 Validating parameters...
2022/10/13 15:19:45 No project ID specified, trying to get default project ID...
2022/10/13 15:19:45 Using project ID found in application detault credencials: my-project
2022/10/13 15:19:45 Filter: (labels.bytewax_io-managed_by = waxctl) AND (name = production-dataflow-2)
2022/10/13 15:19:46 Creating cloud-init config file...
Created production-dataflow-2 instance in zone us-central1-a of project my-project running /var/bytewax/examples/production.py script.
```
