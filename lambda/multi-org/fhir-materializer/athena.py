import time
import uuid

import boto3


GLUE_DATABASE = 'penguin_health_analytics'
ATHENA_WORKGROUP_PREFIX = 'penguin-health-analytics-'
ATHENA_POLL_INTERVAL_SEC = 1.0
ATHENA_TIMEOUT_SEC = 60.0


athena_client = boto3.client('athena')


class AthenaQueryError(Exception):
    pass


def table_suffix(org_id):
    return org_id.replace('-', '_')


def run_query(sql, org_id):
    workgroup = f"{ATHENA_WORKGROUP_PREFIX}{org_id}"
    start = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={'Database': GLUE_DATABASE},
        ClientRequestToken=str(uuid.uuid4()),
    )
    qid = start['QueryExecutionId']

    deadline = time.monotonic() + ATHENA_TIMEOUT_SEC
    while True:
        exec_resp = athena_client.get_query_execution(QueryExecutionId=qid)
        status = exec_resp['QueryExecution']['Status']
        state = status['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = status.get('StateChangeReason', 'no reason given')
            raise AthenaQueryError(f"Athena {state.lower()}: {reason}")
        if time.monotonic() >= deadline:
            try:
                athena_client.stop_query_execution(QueryExecutionId=qid)
            except Exception:
                pass
            raise AthenaQueryError(f"Athena timed out after {ATHENA_TIMEOUT_SEC:.0f}s")
        time.sleep(ATHENA_POLL_INTERVAL_SEC)

    return _fetch_single_column(qid)


def _fetch_single_column(qid):
    values = []
    next_token = None
    first_page = True
    while True:
        kwargs = {'QueryExecutionId': qid, 'MaxResults': 1000}
        if next_token:
            kwargs['NextToken'] = next_token
        resp = athena_client.get_query_results(**kwargs)
        rows = resp['ResultSet']['Rows']
        if first_page and rows:
            rows = rows[1:]
            first_page = False
        for row in rows:
            cells = row.get('Data', [])
            if not cells:
                continue
            value = cells[0].get('VarCharValue')
            if value is None or value == '':
                continue
            values.append(value)
        next_token = resp.get('NextToken')
        if not next_token:
            break
    return values


def table_exists(org_id, table_name):
    glue = boto3.client('glue')
    try:
        glue.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False
