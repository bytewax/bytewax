Waxctl helps you run and manage your Bytewax dataflows in Kubernetes.
It uses the current `kubectl` context configuration, so you need kubectl configured to access the desired Kubernetes cluster for your dataflows.

Check `kubectl` documentation [here](https://kubernetes.io/docs/tasks/tools/#kubectl) if you don't have it installed yet.

## Installation

Installing Waxctl is very simple. You just need to download the binary corresponding to your operating system and architecture [here](/downloads/).

## Dataflow Lifecycle

Waxctl allows you to manage the entire dataflow program lifecycle which includes these phases:

- Deployment
- Getting Status
- Modification
- Deletion

In the following sections we are going to cover each one. 

## Available Commands

For now Waxctl only has one available command and that is `dataflow`. That command is also aliased to `df` so any of them can be used.

You can manage the entire lifecycle of a dataflow in Kubernetes. To do so, the `dataflow` command has the following sub-commands:

- delete
- deploy
- list

Running `dataflow --help` will show further details for these:

```bash
❯ waxctl dataflow --help
Manage dataflows in Kubernetes.

Usage:
  waxctl dataflow [command]

Aliases:
  dataflow, df

Available Commands:
  delete      delete a dataflow
  deploy      deploy a dataflow to Kubernetes creating or upgrading it resources
  list        list dataflows deployed

Flags:
  -h, --help   help for dataflow

Global Flags:
      --debug   enable verbose output

Use "waxctl dataflow [command] --help" for more information about a command.
```

## Waxctl and Namespaces

Waxctl behavior leverages namespaces similarly to other kubernetes tools like helm. If you don’t specify a namespace, the tool will use the current `kubectl` namespace.

You can specify an explicit namespace in every Waxctl command using the `--namespace` flag.

## Deploying a Dataflow

To deploy a dataflow you just need to run `dataflow deploy` passing the path of your python script as an argument. If left unset, Waxctl will use the default name for your dataflow, which is `bytewax`. 

In our example we are going to deploy a dataflow called `my-dataflow` in the current namespace which is `bytewax` in our case:

```bash
❯ waxctl df deploy /var/bytewax/examples/basic.py --name my-dataflow
Dataflow my-dataflow deployed in bytewax namespace.
```

In the above example, Waxctl used the default values for all the flags besides `name`. The tool allows you to configure a wide range of characteristics of your dataflow. 

We can see which flags are available geting the `dataflow deploy` help.
```bash
❯ waxctl df deploy --help
Deploy a dataflow to Kubernetes using the Bytewax helm chart.

The resources are going to be created if the dataflow doesn't exist or 
upgraded if the dataflow is already deployed in the Kubernetes cluster.

The deploy command expects only one argument, a path of a file which could be a
python script or a tar file which must contain your script file (and normally one
or more files needed by your script):

Examples:
  # Deploy a dataflow in current namespace running my-script.py file.
  waxctl dataflow deploy my-script.py

  # Deploy a dataflow in my-namespace namespace running my-script.py file.
  waxctl dataflow deploy my-script.py --namespace my-namespace

  # Deploy a dataflow running my-script.py file contained in my-files.tar file.
  waxctl dataflow deploy my-files.tar -f my-script.py

Usage:
  waxctl dataflow deploy [PATH] [flags]

Flags:
      --create-namespace              create namespace if it does not exist (default true)
      --dry-run                       output the manifests generated without installing them to Kubernetes
  -h, --help                          help for deploy
  -s, --image-pull-secret string      kubernetes secret name to pull custom image (default "default-credentials")
  -i, --image-repository string       custom container image repository URI (default "bytewax/bytewax")
  -t, --image-tag string              custom container image tag (default "latest")
  -N, --name string                   name of the dataflow to deploy in Kubernetes (default "bytewax")
  -n, --namespace string              namespace of Kubernetes resources to deploy
  -o, --output-format output-format   output format for --dry-run; can be 'yaml' or 'json' (default yaml)
  -p, --processes int                 number of processes to deploy. (default 1)
  -f, --python-file-name string       python script file to run. Only needed when [PATH] is a tar file
  -w, --workers int                   number of workers to run in each process (default 1)

Global Flags:
      --debug   enable verbose output
```

## Getting Dataflow Information

You can know which dataflows are deployed in your Kubernetes cluster using the `dataflow list` sub-command. In the output we are going to find details about each dataflow as we can see in this example:

```bash
❯ waxctl df ls
[
  {
    "name": "my-dataflow",
    "namespace": "bytewax",
    "containerImage": "bytewax/bytewax:latest",
    "containerImagePullSecret": "default-credentials",
    "pythonScriptFile": "/var/bytewax/basic.py",
    "processes": "1",
    "processesReady": "1",
    "workersPerProcess": "1",
    "creationTimestamp": "2022-04-18T10:51:03-03:00"
  }
]
```

This is the help text of that command:
```bash
❯ waxctl df list --help
List dataflows deployed.

Examples:
  # List dataflows in current namespace
  waxctl dataflow list

  # List dataflows in bytewax namespace
  waxctl dataflow ls -n bytewax

  # List dataflows across all namespaces
  waxctl dataflow ls -A

Usage:
  waxctl dataflow list [flags]

Aliases:
  list, ls

Flags:
  -A, --all-namespaces     If present, list the Bytewax dataflows across all namespaces
  -h, --help               help for list
  -n, --namespace string   Namespace of the Bytewax dataflows to list

Global Flags:
      --debug   enable verbose output
```

## Updating a Dataflow

To modify a dataflow configuration you need to run `dataflow deploy` again setting the new desired flags configutarion.

Continuing with our example, we could set three processes instead of one running this:

```bash
❯ waxctl df deploy --name my-dataflow /var/bytewax/examples/basic.py -p 3
Dataflow my-dataflow deployed in bytewax namespace.
```

And if we get the list of dataflows we are going to see the new configuration applied:

```bash
❯ waxctl df ls
[
  {
    "name": "my-dataflow",
    "namespace": "bytewax",
    "containerImage": "bytewax/bytewax:latest",
    "containerImagePullSecret": "default-credentials",
    "pythonScriptFile": "/var/bytewax/basic.py",
    "processes": "3",
    "processesReady": "3",
    "workersPerProcess": "1",
    "creationTimestamp": "2022-04-18T10:51:03-03:00"
  }
]
```

You can change any of the flags of your dataflow.

## Removing a Dataflow

To remove the Kubernetes resources of a dataflow you need to run `waxctl dataflow delete` passing the name of the dataflow. 

To run a dry-run delete of our dataflow example we can run this:
```bash
❯ waxctl df rm --name my-dataflow
Dataflow my-dataflow found in bytewax namespace.

Must specify --yes to delete it. 
```

And if we want to actually delete the dataflow we need add the `--yes` flag:
```bash
❯ waxctl df rm --name my-dataflow --yes
Dataflow my-dataflow deleted.
```

## Bytewax Helm Chart

Waxctl uses a compiled-in [Bytewax helm chart](https://github.com/bytewax/helm-charts) to generate all the manifests except the Namespace and ConfigMap which are created by making calls to Kubernetes API directly. 

You can check the entire architecture that Waxctl deploys in our [Bytewax Ecosystem on Kubernetes](/docs/deployment/k8s-ecosystem/) section.

## More Examples

This section covers some of the most common scenarios.

### Setting Processes and Workers per Process

deploying a dataflow running five processes and two workers per process:

```bash
❯ waxctl dataflow deploy my-script.py \
  --name=cluster \
  --processes=5
  --workers=2
```

Using available alias of commands and flags, you can get the same running:

```bash
❯ waxctl df deploy my-script.py -Ncluster -p5 -w2
```

Note that when you use one-character flags you can ommit the `=` or the space between the flag and the value.

### Using a tar file to work with a tree of directories and files

When your python script needs to read other files you can manage that by creating a tarball file with all the files needed and passing the path of that file as the argument to the `deploy` command. Also, you will need to add the flag `--python-file-name` (or its equivalent alias `-f`) with the relative path of your python script inside the tarball.

```bash
❯ waxctl df deploy my-tree.tar -f ./my-script.py --name my-dataflow
```

### Dry-run Flag

running a command with the `--dry-run` flag to get all the manifests without actually deploy them to the Kubernetes cluster:

```bash
❯ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run
```
setting the `-n` flag with the name of a non-existing namespace will cause Waxctl to handle creating the resource and because we use the `--dry-run` flag it is going to include the namespace manifest in the output.

You could also get the output in JSON format:

```bash
❯ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run -ojson
```

And if you need to apply the output manifests to Kubernetes you could run the following:
```bash
❯ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run | kubectl apply -f -
```

### Using a Custom Image from a Private Registry

In our [Including Custom Dependencies in an Image](/docs/deployment/container#including-custom-dependencies-in-an-image) section we describe how to create your own Docker image using a Bytewax image as base.

Let's assume you created a custom image that you pushed to a private image registry in GitLab. In our example the image URL is:

```
registry.gitlab.com/someuser/somerepo:bytewax-example
```

First, we would create a namespace and a Kubernetes `docker-registry` secret with the registry credentials:

```bash
❯ export GITLAB_PAT=your-personal-access-token
❯ kubectl create ns private
❯ kubectl create secret docker-registry gitlab-credentials \
  -n private \
  --docker-server=https://registry.gitlab.com \
  --docker-username=your-email@mail.com \
  --docker-password=$GITLAB_PAT \
  --docker-email=your-email@mail.com
```
After that you can deploy your dataflow with these flags:

```bash
❯ waxctl df deploy ./my-script.py \
  --name df-private \
  --image-repository registry.gitlab.com/someuser/somerepo \
  --image-tag bytewax-example \
  --image-pull-secret gitlab-credentials \
  --namespace private
```
