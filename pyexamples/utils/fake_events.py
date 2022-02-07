import json

from fake_web_events import Simulation

# use the user pool size, sessions per day and the duration to
# create a simulation size that makes sense
def generate_web_events():
    simulation = Simulation(user_pool_size=5, sessions_per_day=100)
    events = simulation.run(duration_seconds=10)

    for event in events:
        yield json.dumps(event)
