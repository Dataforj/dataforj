from dataforj.dataflow import Dataflow

flow: Dataflow = None

def set_flow(new_flow: Dataflow):
    global flow
    flow = new_flow

def get_flow():
    return flow
