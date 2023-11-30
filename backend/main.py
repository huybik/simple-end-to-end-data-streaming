from datetime import datetime
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

app = FastAPI()

cluster = Cluster(['localhost'])  # Replace with your Cassandra node IP
session = cluster.connect('spark_streams')  # Replace with your keyspace name

def query_request_excutor(session, project_id, from_timestamp, end_timestamp, event_type):
    query = f"""
        SELECT * FROM aggregated_api_request
        WHERE project_id = '{project_id}'
        AND timestamp >= '{from_timestamp}'
        ;
    """
    statement = SimpleStatement(query)
    result = session.execute(statement)

    return result

# Close the session when the application shuts down
@app.on_event("shutdown")
def shutdown_event():
    session.shutdown()

@app.get("/api/request-chart")
async def get_request_count(project_id: str = None, from_timestamp: str = None, end_timestamp: str = None):
    # Filter DataFrame based on parameters
    from_timestamp_formated = (datetime.utcfromtimestamp(int(from_timestamp))).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    end_timestamp_formated = (datetime.utcfromtimestamp(int(end_timestamp))).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    result = query_request_excutor(session, project_id, from_timestamp_formated, end_timestamp_formated)

    # Calculate request count
    df = pd.DataFrame(result)
    # df['session_end'] = pd.to_datetime(df['session_end'])
    # df['minute'] = df['session_end'].dt.minute
    # df['hour'] = df['session_end'].dt.hour
    # df['day'] = df['session_end'].dt.day
    # df['month'] = df['session_end'].dt.month
    # df['year'] = df['session_end'].dt.year

    request_count = df['count'].sum()

    # Convert result to JSON
    result_json = {
        'project_id': project_id,
        'from_timestamp': from_timestamp_formated,
        'end_timestamp': end_timestamp_formated,
        'request_count': int(request_count)
    }

    return JSONResponse(content=result_json)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
