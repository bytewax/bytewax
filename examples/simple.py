from bytewax import Dataflow, run

flow = Dataflow()
flow.map(lambda x: x * x)
flow.capture()

if __name__ == "__main__":
    for epoch, y in sorted(run(flow, range(100))):
        print(epoch, y)
