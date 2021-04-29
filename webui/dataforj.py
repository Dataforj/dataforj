from dataforj.dataflow import Dataflow

# One Flow, one DF and one Schema stored in memory for the server.  Obviously this 
# will need to be refactored into a better solution later.
flow: Dataflow = None
schema = None
df = None

def set_flow(new_flow: Dataflow):
    '''Sets the flow that will be shown to the User in the UI'''
    global flow
    flow = new_flow


def get_flow():
    '''Gets the flow that is currently being worked on'''
    return flow


def load_schema(step_name: str):
    '''Load a schema from a file in the Dataforj project'''
    global schema
    schema_location = get_flow()._steps[step_name].schema_location
    schema = read_schema_from_file(schema_location)


def set_schema(new_schema): 
    '''Set the schema after it has been created or updated by the User'''
    global schema
    schema = new_schema


def get_schema():
    '''Get the schema to show it to the user'''
    return schema


def set_df(new_df): 
    '''Set the df'''
    global df
    df = new_df


def get_df():
    '''Get the df'''
    return df
