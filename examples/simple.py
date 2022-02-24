from bytewax import Dataflow, run

flow = Dataflow()
flow.map(lambda x: x * x)
flow.capture()

if __name__ == "__main__":
    for epoch, y in sorted(run(flow, enumerate(range(10)))):
        print(y)
