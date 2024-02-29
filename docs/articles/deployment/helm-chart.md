# Helm Chart

<!-- (below is old K8S ecosystem) -->

As we mentioned in our <project:#cluster>. Bytewax allows you to run a
dataflow program in a coordinated set of processes.

That entry point requires the addresses of all the processes and
assign them each a unique ID.

Our Kubernetes implementation is coupled with
`bytewax.parse.proc_env()` function which looks for some specific
environments variables to fill the needed arguments of
`bytewax.cluster_main()`.

We modeled the following Kubernetes ecosystem to run a Bytewax
dataflow with one or more processes involved.

## Kubernetes Resources

This diagram shows which Kubernetes resources are included in our
stack.

![Bytewax on Kubernetes](/assets/k8s_ecosystem.png)

We are going to cover each resource by explaining how we use them to
run a Bytewax dataflow:

## Namespace

A Bytewax dataflow stack can be deployed in any namespace but it is
recommended that it is installed in a dedicated one to leverage
Namespace isolation characteristics about security and resource
designation.

## Dataflow StatefulSet

Bytewax uses a StatefulSet object to manage workloads in Kubernetes
because a unique and stable network identifier for each Pod is
required (stable means that a Pod is going to keep its name across Pod
(re)scheduling) and StatefulSets meet this requirement.

Because each Pod in a StatefulSet derives its hostname from the name
of the StatefulSet and the ordinal of the Pod, we can have an
Init-Container which dynamically generates the needed list of
addresses and stores them in a file in a volume which will be mounted
by the application container.

Also, we use the ordinal of each Pod as the unique ID needed for each
process.

For example, assuming that we are running a dataflow named
`my-dataflow` with three processes in a namespace called `bytewax` we
are going to have these pods:

```
my-dataflow-0
my-dataflow-1
my-dataflow-2
```

And their network addresses are going to be:

```
my-dataflow-0.my-dataflow.bytewax.svc.cluster.local:9999
my-dataflow-1.my-dataflow.bytewax.svc.cluster.local:9999
my-dataflow-2.my-dataflow.bytewax.svc.cluster.local:9999
```

As you can see we use the port 9999 to expose each process.

Leveraging the Kubernetes naming convention for Pods and their DNS we
automated the stack configuration.

Another important aspect of each Pod configuration is that the
application container also mounts another volume called
`working-directory`. This is going to have a ConfigMap that we will
cover later, but basically this has the python file and any other
files required to run successfully.

In this image we show how the init-container and the application
container of the Pods work together:

![Bytewax on Kubernetes](/assets/pod_details.png)

The `hostfile` init-container stores a file named `hostfile.txt` with
all the addresses of the processes, and the application container
`process` reads that file (in fact, `bytewax.parse.proc_env()` does
that work).

Also, the image shows that the init-container is reading the content
of the ConfigMap mounted in the `python-files` volume and copying that
to the `working-directory` volume. In the example the content of the
ConfigMap is a file named `basic.py`. In the ConfigMap section we are
going to talk more about that.

### Environment Variables in the Application Container

In the above diagram, the application container has these environment
variables:

```yaml
- name: BYTEWAX_IMPORT_STR
  value: basic:flow
- name: BYTEWAX_WORKDIR
  value: /var/bytewax
- name: BYTEWAX_WORKERS_PER_PROCESS
  value: "1"
- name: BYTEWAX_POD_NAME
  valueFrom:
    fieldRef:
      apiVersion: v1
      fieldPath: metadata.name
- name: BYTEWAX_REPLICAS
  value: "3"
- name: BYTEWAX_KEEP_CONTAINER_ALIVE
  value: "true"
- name: BYTEWAX_HOSTFILE_PATH
  value: /etc/bytewax/hostfile.txt
- name: BYTEWAX_STATEFULSET_NAME
  value: my-dataflow
```

Some of those were already covered in the <project:#container-image>
section.

The environment variables can be grouped into two groups:

#### Environments Variables used in `entrypoint.sh` script:

- `BYTEWAX_IMPORT_STR`: path of the python script to run.

- `BYTEWAX_WORKDIR`: working directory with all the ConfigMap content.

- `BYTEWAX_KEEP_CONTAINER_ALIVE`: if is set to `true`, after the
  dataflow program ends an infinite loop is going to keep the
  container running.

#### Environments Variables internally used by Bytewax when `parse.proc_env()` is called:

- `BYTEWAX_HOSTFILE_PATH`: path of the file that has all the processes
  addresses.

- `BYTEWAX_POD_NAME`: name of the current Pod used to get the ordinal
  of it.

- `BYTEWAX_REPLICAS`: number of processes running in the Bytewax
  cluster.

- `BYTEWAX_STATEFULSET_NAME`: name of the StatefulSet used to get the
  ordinal of the current Pod.

- `BYTEWAX_WORKERS_PER_PROCESS`: number of workers triggered by each
  process.

### Security Configuration

Our StatefulSet Pod template definition has strict Pod and Container
security context settings. These are the most important aspects:

- The container user is not root and can't escalate.

- All the filesystems are read-only except the `working-directory`
  volume.

- The only Linux Capability enabled is `NET_BIND_SERVICE`.

## Headless service

This is a Kubernetes Service without an assigned IP only used to
interface with Kubernetes' service discovery mechanisms.

For Bytewax we use the service to instruct Kubernetes to create an
endpoint for each Pod in our Statefulset. In this way, there will be a
known DNS entry for each process in the Bytewax cluster.

## ConfigMap

We stored the python script file and every needed file in a ConfigMap.
The content of that ConfigMap is mounted in the `working-directory`
volume in the `BYTEWAX_WORKDIR` path with read-write access. This
strategy allows us to:

- Decouple the container image from the Python code to run.

- Set strong security filesystem configurations setting the
  filesystems to read-only except for the working directory.

### Storing a tree of files and directories

Sometimes your python script will need some input extra files to work
with. In that case, we have implemented an approach that involves
creating a tar file including all the files/directories needed and
then storing that tarball in the ConfigMap.

In that scenario we use some extra steps in our init-container. You
can read more about that in our [Helm Chart
repository](https://github.com/bytewax/helm-charts/).

## Registry Credentials Secret

In the case that the container image is stored in a private registry,
this secret will contain the credentials needed to pull images from
that registry.

## Service Account

We use this Kubernetes object to give access to the Registry
Credentials Secrets mentioned above to the StatefulSet Pods.

## Next Steps

If you want to deploy this stack in your Kubernetes cluster we invite
you to read our section <project:/articles/deployment/waxctl.md> where
we use the Bytewax CLI, Waxctl, to do that.

## Example Manifests

_The manifests for the above example_

As you can see, we rely on Helm to generate our manifests. You can
read more about our Helm Chart
[here](https://github.com/bytewax/helm-charts/).

```yaml
---
apiVersion: v1
kind: Namespace
metadata:
  annotations:
    meta.helm.sh/release-name: my-dataflow
    meta.helm.sh/release-namespace: bytewax
  labels:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: bytewax
    bytewax.io/managed-by: waxctl
    kubernetes.io/metadata.name: bytewax
  name: bytewax
spec:
  finalizers:
    - kubernetes
---
apiVersion: v1
data:
  basic.py: |
    import bytewax.operators as op
    from bytewax.connectors.stdio import StdOutSink
    from bytewax.dataflow import Dataflow
    from bytewax.testing import TestingSource


    def double(x: int) -> int:
        return x * 2


    def halve(x: int) -> int:
        return x // 2


    def minus_one(x: int) -> int:
        return x - 1


    def stringy(x: int) -> str:
        return f"<dance>{x}</dance>"


    flow = Dataflow("basic")

    inp = op.input("inp", flow, TestingSource(range(10)))
    branch = op.branch("e_o", inp, lambda x: x % 2 == 0)
    evens = op.map("halve", branch.trues, halve)
    odds = op.map("double", branch.falses, double)
    combo = op.merge("merge", evens, odds)
    combo = op.map("minus_one", combo, minus_one)
    string_output = op.map("stringy", combo, stringy)
    op.output("out", string_output, StdOutSink())
kind: ConfigMap
metadata:
  annotations:
    meta.helm.sh/release-name: my-dataflow
    meta.helm.sh/release-namespace: bytewax
  labels:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: bytewax
    bytewax.io/managed-by: waxctl
  name: my-dataflow
  namespace: bytewax
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  annotations:
    meta.helm.sh/release-name: my-dataflow
    meta.helm.sh/release-namespace: bytewax
  generation: 2
  labels:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: bytewax
    app.kubernetes.io/version: 0.8.0
    bytewax.io/managed-by: waxctl
    helm.sh/chart: bytewax-0.2.3
  name: my-dataflow
  namespace: bytewax
spec:
  podManagementPolicy: Parallel
  replicas: 3
  revisionHistoryLimit: 10
  selector:
    matchLabels:
      app.kubernetes.io/instance: my-dataflow
      app.kubernetes.io/name: bytewax
  serviceName: my-dataflow
  template:
    metadata:
      creationTimestamp: null
      labels:
        app.kubernetes.io/instance: my-dataflow
        app.kubernetes.io/name: bytewax
    spec:
      containers:
        - command:
            - sh
            - -c
            - sh ./entrypoint.sh
          env:
            - name: RUST_LOG
              value: librdkafka=debug,rdkafka::client=debug
            - name: RUST_BACKTRACE
              value: full
            - name: BYTEWAX_PYTHON_FILE_PATH
              value: /var/bytewax/basic.py
            - name: BYTEWAX_WORKDIR
              value: /var/bytewax
            - name: BYTEWAX_WORKERS_PER_PROCESS
              value: "1"
            - name: BYTEWAX_POD_NAME
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.name
            - name: BYTEWAX_REPLICAS
              value: "3"
            - name: BYTEWAX_KEEP_CONTAINER_ALIVE
              value: "true"
            - name: BYTEWAX_HOSTFILE_PATH
              value: /etc/bytewax/hostfile.txt
            - name: BYTEWAX_STATEFULSET_NAME
              value: my-dataflow
          image: bytewax/bytewax:latest
          imagePullPolicy: Always
          name: process
          ports:
            - containerPort: 9999
              name: process
              protocol: TCP
          resources: {}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              add:
                - NET_BIND_SERVICE
              drop:
                - ALL
            readOnlyRootFilesystem: true
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          volumeMounts:
            - mountPath: /etc/bytewax
              name: hostfile
            - mountPath: /var/bytewax/
              name: working-directory
      dnsPolicy: ClusterFirst
      imagePullSecrets:
        - name: default-credentials
      initContainers:
        - command:
            - sh
            - -c
            - |
              set -ex
              # Generate hostfile.txt.
              echo "my-dataflow-0.my-dataflow.bytewax.svc.cluster.local:9999" > /etc/bytewax/hostfile.txt
              replicas=$(($BYTEWAX_REPLICAS-1))
              x=1
              while [ $x -le $replicas ]
              do
                echo "my-dataflow-$x.my-dataflow.bytewax.svc.cluster.local:9999" >> /etc/bytewax/hostfile.txt
                x=$(( $x + 1 ))
              done
              # Copy python files to working directory
              cp /tmp/bytewax/. /var/bytewax -R
              cd /var/bytewax
              tar -xvf *.tar || echo "No tar files found."
          env:
            - name: BYTEWAX_REPLICAS
              value: "3"
          image: busybox
          imagePullPolicy: Always
          name: init-hostfile
          resources: {}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              add:
                - NET_BIND_SERVICE
              drop:
                - ALL
            readOnlyRootFilesystem: true
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          volumeMounts:
            - mountPath: /etc/bytewax
              name: hostfile
            - mountPath: /var/bytewax/
              name: working-directory
            - mountPath: /tmp/bytewax/
              name: python-files
      restartPolicy: Always
      schedulerName: default-scheduler
      securityContext:
        fsGroup: 2000
        runAsGroup: 3000
        runAsNonRoot: true
        runAsUser: 65532
      serviceAccount: my-dataflow-bytewax
      serviceAccountName: my-dataflow-bytewax
      terminationGracePeriodSeconds: 10
      volumes:
        - emptyDir: {}
          name: hostfile
        - configMap:
            defaultMode: 420
            name: my-dataflow
          name: python-files
        - emptyDir: {}
          name: working-directory
  updateStrategy:
    rollingUpdate:
      partition: 0
    type: RollingUpdate
---
apiVersion: v1
kind: Service
metadata:
  annotations:
    meta.helm.sh/release-name: my-dataflow
    meta.helm.sh/release-namespace: bytewax
  labels:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: bytewax
    app.kubernetes.io/version: 0.8.0
    bytewax.io/managed-by: waxctl
    helm.sh/chart: bytewax-0.2.3
  name: my-dataflow
  namespace: bytewax
spec:
  clusterIP: None
  clusterIPs:
    - None
  ipFamilies:
    - IPv4
  ipFamilyPolicy: SingleStack
  ports:
    - name: worker
      port: 9999
      protocol: TCP
      targetPort: 9999
  selector:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/name: bytewax
  sessionAffinity: None
  type: ClusterIP
---
apiVersion: v1
imagePullSecrets:
  - name: default-credentials
kind: ServiceAccount
metadata:
  annotations:
    meta.helm.sh/release-name: my-dataflow
    meta.helm.sh/release-namespace: bytewax
  labels:
    app.kubernetes.io/instance: my-dataflow
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/name: bytewax
    app.kubernetes.io/version: 0.8.0
    bytewax.io/managed-by: waxctl
    helm.sh/chart: bytewax-0.2.3
  name: my-dataflow-bytewax
  namespace: bytewax
secrets:
  - name: my-dataflow-bytewax-token-2nnxc
```
