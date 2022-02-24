from bytewax import Dataflow, parse, run_cluster


def inp():
    for i in range(10):
        yield (0, i)


def double(x):
    return x * 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


def peek(x):
    print(f"peekin at {x}")


flow = Dataflow()
flow.map(double)
flow.map(minus_one)
flow.map(stringy)
flow.capture()


if __name__ == "__main__":
    out = run_cluster(flow, inp(), **parse.cluster_args())
    for epoch, item in out:
        print(epoch, item)
