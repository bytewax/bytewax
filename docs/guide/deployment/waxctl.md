(xref-waxctl)=
# Using `Waxctl Pro` to Run your Dataflow on Kubernetes

Waxctl helps you run and manage your Bytewax dataflows in Kubernetes.
It uses the current `kubectl` context configuration, so you need
kubectl configured to access the desired Kubernetes cluster for your
dataflows.

Check `kubectl` documentation
[here](https://kubernetes.io/docs/tasks/tools/#kubectl) if you don't
have it installed yet.

## Installation

Installing Waxctl is very simple. You just need to download the binary
corresponding to your operating system and architecture
[here](https://bytewax.io/downloads).

## Dataflow Lifecycle

Waxctl allows you to manage the entire dataflow program lifecycle
which includes these phases:

- Deployment

- Getting Status

- Modification

- Deletion

In the following sections we are going to cover each one.

## Available Commands

For now Waxctl only has one available command and that is `dataflow`.
That command is also aliased to `df` so any of them can be used.

You can manage the entire lifecycle of a dataflow in Kubernetes. To do
so, the `dataflow` command has the following sub-commands:

- `delete`

- `deploy`

- `list`

Running `dataflow --help` will show further details for these:

```console
$ waxctl dataflow --help
Waxctl Pro version 0.12.0

Manage dataflows running on Kubernetes.
Waxctl uses the current kubectl context configuration, so you need kubectl configured to access the desired Kubernetes cluster for your dataflows.

Usage:
  waxctl dataflow [flags]
  waxctl dataflow [command]

Aliases:
  dataflow, df, k8s

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

Waxctl behavior leverages namespaces similarly to other kubernetes
tools like helm. If you don’t specify a namespace, the tool will use
the current `kubectl` namespace.

You can specify an explicit namespace in every Waxctl command using
the `--namespace` flag.

## Deploying a Dataflow

To deploy a dataflow you just need to run `dataflow deploy` passing
the path of your python script as an argument. If left unset, Waxctl
will use the default name for your dataflow, which is `bytewax`.

In our example we are going to deploy a dataflow called `my-dataflow`
in the current namespace which is `bytewax` in our case:

```console
$ waxctl df deploy /var/bytewax/examples/basic.py --name my-dataflow
Dataflow my-dataflow deployed in bytewax namespace.
```

In the above example, Waxctl used the default values for all the flags
besides `name`. The tool allows you to configure a wide range of
characteristics of your dataflow.

We can see which flags are available geting the `dataflow deploy`
help.

```console
$ waxctl df deploy --help
Waxctl Pro version 0.12.0

Deploy a dataflow to Kubernetes using the Bytewax helm chart.

The resources are going to be created if the dataflow doesn't exist or upgraded if the dataflow is already deployed in the Kubernetes cluster.

The deploy command expects only one argument, a path or URI of a file which could be a python script or a tar file which must contain your script file (and normally one or more files needed by your script):

Examples:
  # Deploy a dataflow in current namespace running my-script.py file.
  waxctl dataflow deploy my-script.py

  # Deploy a dataflow in current namespace running dataflow.py file.
  waxctl dataflow deploy https://raw.githubusercontent.com/my-user/my-repo/main/dataflow.py

  # Deploy a dataflow in my-namespace namespace running my-script.py file.
  waxctl dataflow deploy my-script.py --namespace my-namespace

  # Deploy a dataflow running my-script.py file contained in my-files.tar file.
  waxctl dataflow deploy my-files.tar -f my-script.py

  # Deploy a dataflow in current namespace running my-script.py file with the environment variable 'SERVER'.
  waxctl dataflow deploy my-script.py -e SERVER=localhost:1433

Usage:
  waxctl dataflow deploy [PATH] [flags]

Flags:
  -V, --chart-values-file string                          load Bytewax helm chart values from a file. See more at https://bytewax.github.io/helm-charts
      --create-namespace                                  create namespace if it does not exist (default true)
      --dry-run                                           output the manifests generated without installing them to Kubernetes
  -e, --environment-variables strings                     environment variables to inject to dataflow container. The format must be KEY=VALUE
  -h, --help                                              help for deploy
  -l, --image-pull-policy string                          custom container image pull policy (the value must be Always, IfNotPresent or Never) (default "Always")
  -s, --image-pull-secret string                          kubernetes secret name to pull custom image (default "default-credentials")
  -i, --image-repository string                           custom container image repository URI (default "bytewax/bytewax")
  -t, --image-tag string                                  custom container image tag (default "latest")
      --job-mode                                          run a Job in Kubernetes instead of an Statefulset. Use this to batch processing
      --keep-alive                                        keep the container alive after dataflow ends. It could be useful for troubleshooting purposes
  -N, --name string                                       name of the dataflow to deploy in Kubernetes (default "bytewax")
  -n, --namespace string                                  namespace of Kubernetes resources to deploy
  -o, --output-format output-format                       output format for --dry-run; can be 'yaml' or 'json' (default yaml)
      --platform                                          deploy the dataflow as a bytewax.io/dataflow Custom Resource (requires Bytewax Platform installed)
  -p, --processes int                                     number of processes to deploy (default 1)
  -f, --python-file-name string                           python script file to run. Only needed when [PATH] is a tar file
      --recovery                                          stores recovery files in Kubernetes persistent volumes to allow resuming after a restart (your dataflow must have recovery enabled: https://bytewax.io/docs/getting-started/recovery)
      --recovery-backup                                   back up worker state DBs to cloud storage (must have recovery flag present and provide S3 parameters)
      --recovery-backup-interval int                      System time duration in seconds to keep extra state snapshots around
      --recovery-backup-s3-aws-access-key-id string       AWS credentials access key id
      --recovery-backup-s3-aws-secret-access-key string   AWS credentials secret access key
      --recovery-backup-s3-k8s-secret string              name of the Kubernetes secret storing AWS credentials.
      --recovery-backup-s3-url string                     s3 url to store state backups. For example, s3://mybucket/mydataflow-state-backups
      --recovery-parts int                                number of recovery parts (default 1)
      --recovery-single-volume                            use only one persistent volume for all dataflow's pods in Kubernetes
      --recovery-size string                              size of the persistent volume claim to be assign to each dataflow pod in Kubernetes (default "10Gi")
      --recovery-snapshot-interval int                    system time duration in seconds to snapshot state for recovery
      --recovery-storageclass string                      storage class of the persistent volume claim to be assign to each dataflow pod in Kubernetes
  -r, --requirements-file-name string                     requirements.txt file if needed
  -v, --values string                                     load parameter values from a config file (supported formats: JSON, TOML, YAML, HCL, envfile and Java properties)
  -w, --workers int                                       number of workers to run in each process (default 1)
      --yes                                               confirm the update and restart of the dataflow in the Kubernetes cluster

Global Flags:
      --debug   enable verbose output
```

## Getting Dataflow Information

You can know which dataflows are deployed in your Kubernetes cluster
using the `dataflow list` sub-command. In the output we are going to
find details about each dataflow as we can see in this example:

```console
$ waxctl df ls
Dataflow    Namespace Python File    Image                  Processes Source   Creation Time
my-dataflow bytewax   basic.py       bytewax/bytewax:latest 1/1       waxctl   2024-08-20T08:32:40-03:00
```

This is the help text of that command:

```console
$ waxctl df list --help
Waxctl Pro version 0.12.0

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
  -A, --all-namespaces     If present, list the Bytewax dataflows across all namespaces.
  -h, --help               help for list
  -N, --name string        name of the dataflow to display.
  -n, --namespace string   Namespace of the Bytewax dataflows to list.
  -v, --verbose            return detailed information of the dataflows.

Global Flags:
      --debug   enable verbose output
```

## Updating a Dataflow

To modify a dataflow configuration you need to run `dataflow deploy`
again setting the new desired flags configuration.

Continuing with our example, we could set three processes instead of
one running this:

```console
$ waxctl df deploy --name my-dataflow /var/bytewax/examples/basic.py -p 3 --yes
Dataflow my-dataflow deployed in bytewax namespace.
```

And if we get the list of dataflows we are going to see the new
configuration applied:

```console
$ waxctl df ls
Dataflow    Namespace Python File    Image                  Processes Source   Creation Time
my-dataflow bytewax   basic.py       bytewax/bytewax:latest 3/3       waxctl   2024-08-20T08:32:40-03:00
```

You can change any of the flags of your dataflow.

## Removing a Dataflow

To remove the Kubernetes resources of a dataflow you need to run
`waxctl dataflow delete` passing the name of the dataflow.

To run a dry-run delete of our dataflow example we can run this:

```console
$ waxctl df rm my-dataflow
Dataflow my-dataflow found in bytewax namespace.

Must specify --yes to delete it.
```

And if we want to actually delete the dataflow we need add the `--yes`
flag:

```console
$ waxctl df rm my-dataflow --yes
Dataflow my-dataflow deleted.
```

## Bytewax Helm Chart

Waxctl uses a compiled-in [Bytewax helm
chart](https://github.com/bytewax/helm-charts) to generate all the
manifests except the Namespace and ConfigMap which are created by
making calls to Kubernetes API directly.

## More Examples

This section covers some of the most common scenarios.

### Setting Processes and Workers per Process

Deploying a dataflow running five processes and two workers per
process:

```console
$ waxctl dataflow deploy my-script.py \
  --name=cluster \
  --processes=5 \
  --workers=2
```

Using available alias of commands and flags, you can get the same
running:

```console
$ waxctl df deploy my-script.py -Ncluster -p5 -w2
```

Note that when you use one-character flags you can ommit the `=` or
the space between the flag and the value.

### Using a tar file to work with a tree of directories and files

When your python script needs to read other files you can manage that
by creating a tarball file with all the files needed and passing the
path of that file as the argument to the `deploy` command. Also, you
will need to add the flag `--python-file-name` (or its equivalent
alias `-f`) with the relative path of your python script inside the
tarball.

```console
$ waxctl df deploy my-tree.tar -f ./my-script.py --name my-dataflow
```

### Dry-run Flag

Running a command with the `--dry-run` flag to get all the manifests
without actually deploy them to the Kubernetes cluster:

```console
$ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run
```

Setting the `-n` flag with the name of a non-existing namespace will
cause Waxctl to handle creating the resource and because we use the
`--dry-run` flag it is going to include the namespace manifest in the
output.

You could also get the output in JSON format:

```console
$ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run -ojson
```

And if you need to apply the output manifests to Kubernetes you could
run the following:

```console
$ waxctl df deploy ./basic.py -N dataflow -n new-namespace --dry-run | kubectl apply -f -
```

### Using a Custom Image from a Private Registry

In our <project:#xref-container-custom-deps> section we describe how
to create your own Docker image using a Bytewax image as base.

Let's assume you created a custom image that you pushed to a private
image registry in GitLab. In our example the image URL is:

```
registry.gitlab.com/someuser/somerepo:bytewax-example
```

First, we would create a namespace and a Kubernetes `docker-registry`
secret with the registry credentials:

```console
$ export GITLAB_PAT=your-personal-access-token
$ kubectl create ns private
$ kubectl create secret docker-registry gitlab-credentials \
  -n private \
  --docker-server=https://registry.gitlab.com \
  --docker-username=your-email@mail.com \
  --docker-password=$GITLAB_PAT \
  --docker-email=your-email@mail.com
```

After that you can deploy your dataflow with these flags:

```console
$ waxctl df deploy ./my-script.py \
  --name df-private \
  --image-repository registry.gitlab.com/someuser/somerepo \
  --image-tag bytewax-example \
  --image-pull-secret gitlab-credentials \
  --namespace private
```
