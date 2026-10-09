"""通过 SSH 标准输入运行，仅执行数据库只读查询。"""
import json
import re
import subprocess
from datetime import datetime, timezone, timedelta

SOURCES = {
    "webchat": ("sub2api", "webchat_response_requests", False, True),
    "audio": ("sub2api", "webchat_audio_transcription_requests", False, True),
    "gateway": ("sub2api", "ops_error_logs", False, True),
    "message": ("openwebui", "chat_message", True, False),
    "tool": ("openwebui", "sub2api_managed_sandbox_command", True, False),
}


def timestamp(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("游标时间必须包含时区")
    return dt.astimezone(timezone.utc)


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def upstream_account_sql(source):
    # Keep every recorded account/evidence pair; never borrow a parent account.
    if source in {'webchat','audio'}:
        evidence = """SELECT r.metadata->'settlement_context'->>'account_id' account_id,'settlement_context' evidence
            UNION SELECT r.metadata->'failure'->>'last_account_id','failure_last_account'
            UNION SELECT u.account_id::text,'usage_log' FROM usage_logs u
                WHERE u.request_id=r.request_id AND u.user_id=r.user_id
            UNION SELECT e.account_id::text,'gateway_error' FROM ops_error_logs e
                WHERE (e.request_id=r.request_id OR e.client_request_id=r.request_id) AND e.user_id=r.user_id"""
    elif source == 'gateway':
        evidence = "SELECT r.account_id::text account_id,'gateway_error' evidence"
    else:
        return "'[]'::json"
    return """(SELECT coalesce(json_agg(json_build_object(
        'account_id',evidence.account_id,'name',a.name,'platform',a.platform,
        'type',a.type,'evidence',evidence.evidence) ORDER BY evidence.account_id,evidence.evidence),'[]'::json)
        FROM (""" + evidence + """) evidence LEFT JOIN accounts a ON a.id::text=evidence.account_id
        WHERE evidence.account_id ~ '^[1-9][0-9]*$')"""


def build_query(source, cursor, upper, limit):
    if source not in SOURCES:
        raise ValueError("未知日志来源")
    database, table, epoch, numeric_id = SOURCES[source]
    start, end = timestamp(cursor["time"]), timestamp(upper)
    cursor_id = str(cursor.get("id", "0" if numeric_id else ""))
    if numeric_id and not re.fullmatch(r"\d+", cursor_id):
        raise ValueError("无效游标编号")
    limit = max(1,min(int(limit),2000))
    column = "created_at" if source == "gateway" else "updated_at"
    time_col = "r."+column
    lower_sql = str(int(start.timestamp())) if epoch else literal(start.isoformat())+"::timestamptz"
    upper_sql = str(int(end.timestamp())) if epoch else literal(end.isoformat())+"::timestamptz"
    id_sql = cursor_id if numeric_id else literal(cursor_id)
    where = f"({time_col},r.id)>({lower_sql},{id_sql}) AND {time_col}<={upper_sql}"
    date = lambda c: f"to_char({'to_timestamp(r.'+c+')' if epoch else 'r.'+c} AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')"
    common = f"r.id::text id,{literal(source)} source,{date('created_at')} created_at,{date(column)} updated_at"
    if source in {"webchat","audio"}:
        conv = "r.conversation_id" if source == "webchat" else "NULL::text"
        select = f"{common},r.user_id::text user_id,r.request_id,{conv} conversation_id,r.model,r.status,r.error_code,r.error_message,r.webchat_session_id::text session_id,r.metadata->>'auxiliary_kind' auxiliary_kind,r.metadata->>'trigger_request_id' trigger_request_id,coalesce(nullif(r.metadata->>'parent_request_id',''),nullif(r.metadata->'managed_function_calling'->>'parent_request_id','')) parent_request_id,r.metadata->'managed_function_calling'->>'continuation_items' continuation_items"
        joins = ""
    elif source == "gateway":
        select = f"{common},r.user_id::text user_id,r.request_id,r.client_request_id,r.model,'failed' status,r.error_type error_code,r.error_message,r.error_phase,r.status_code,r.upstream_status_code,r.upstream_error_message,r.network_error_type,r.duration_ms,r.request_path"
        joins = ""
        where += " AND (r.request_path LIKE '/api/v1/webchat/%' OR r.request_path LIKE '/webchat/%' OR EXISTS (SELECT 1 FROM webchat_response_requests w WHERE w.request_id=r.request_id OR w.request_id=r.client_request_id) OR EXISTS (SELECT 1 FROM webchat_audio_transcription_requests a WHERE a.request_id=r.request_id OR a.request_id=r.client_request_id))"
    elif source == "message":
        select = f"""{common},coalesce(link.sub2api_user_id,'openwebui:'||r.user_id) user_id,u.name user_name,u.email user_email,
            r.user_id openwebui_user_id,CASE WHEN link.sub2api_user_id IS NULL THEN 'unattributed' ELSE 'unique_identity' END mapping_status,
            r.chat_id conversation_id,r.model_id model,
            CASE WHEN r.error IS NOT NULL AND r.error::text NOT IN ('null','{{}}','[]','""') THEN 'failed' WHEN r.done THEN 'completed' ELSE 'running' END status,
            CASE WHEN r.error IS NOT NULL AND r.error::text NOT IN ('null','{{}}','[]','""') THEN r.error::text ELSE NULL END error_message,
            r.status_history,r.usage,r.role"""
        joins = """ LEFT JOIN "user" u ON u.id=r.user_id LEFT JOIN LATERAL (SELECT CASE WHEN count(DISTINCT nullif(s.sub2api_user_id,''))=1 THEN min(nullif(s.sub2api_user_id,'')) END sub2api_user_id FROM sub2api_webchat_session s WHERE s.user_id=r.user_id) link ON true"""
        where += " AND r.role='assistant'"
    else:
        select = f"""{common},coalesce(link.sub2api_user_id,'openwebui:'||s.user_id) user_id,u.name user_name,u.email user_email,s.user_id openwebui_user_id,CASE WHEN link.sub2api_user_id IS NULL THEN 'unattributed' ELSE 'unique_identity' END mapping_status,s.chat_id conversation_id,s.message_id,r.call_id,r.session_id,r.status,
            CASE WHEN r.status IN ('failed','error','timeout') THEN left(r.result,16000) ELSE NULL END error_message,left(r.logs,24000) logs,left(r.result,16000) result,
            length(r.logs)>24000 logs_truncated,length(r.result)>16000 result_truncated"""
        joins = """ LEFT JOIN sub2api_managed_sandbox_session s ON s.id=r.session_id LEFT JOIN "user" u ON u.id=s.user_id LEFT JOIN LATERAL (SELECT CASE WHEN count(DISTINCT nullif(x.sub2api_user_id,''))=1 THEN min(nullif(x.sub2api_user_id,'')) END sub2api_user_id FROM sub2api_webchat_session x WHERE x.user_id=s.user_id) link ON true"""
    select += "," + upstream_account_sql(source) + " upstream_accounts"
    return f"SELECT {select} FROM {table} r{joins} WHERE {where} ORDER BY {time_col},r.id LIMIT {limit}"


def query(config, database, sql):
    command = ['docker','exec','-i',config['db_container'],'psql','-p',str(config['db_port']),'-U',config['db_user'],'-d',database,'-X','-q','-A','-t','-v','ON_ERROR_STOP=1']
    wrapped = "BEGIN READ ONLY; SET LOCAL statement_timeout='8000ms'; SET LOCAL lock_timeout='1000ms'; SET LOCAL TIME ZONE 'UTC'; SELECT coalesce(json_agg(row_to_json(data)),'[]'::json) FROM ("+sql+") data; COMMIT;"
    result = subprocess.run(command,input=wrapped,capture_output=True,text=True,timeout=15)
    if result.returncode:
        raise RuntimeError(result.stderr.strip()[:1200])
    return json.loads(result.stdout.strip())


def collect(payload):
    config = dict(payload['config'])
    config['batch_size'] = max(1, min(int(config['batch_size']), 2000))
    # 以数据库时间设置水位，并保留延迟提交的重叠采集窗口。
    now = query(config,'sub2api',"SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"') time")[0]['time']
    upper = (timestamp(now)-timedelta(seconds=3)).isoformat()
    output = {'server_time':now,'sources':{}}
    if payload.get('include_users'):
        try:
            output['users'] = query(config,'sub2api',"SELECT id::text id,coalesce(nullif(username,''),email) name,email FROM users ORDER BY id")
        except Exception as exc:
            output['users_error'] = str(exc)
    for source, requested in payload['cursors'].items():
        try:
            bound = requested.get('upper') or upper
            if timestamp(bound) < timestamp(requested['time']):
                raise ValueError('采集上界早于游标，请检查服务器时钟或断点')
            items = query(config,SOURCES[source][0],build_query(source,requested,bound,config['batch_size']))
            has_more = len(items) == config['batch_size']
            if has_more:
                last = items[-1]
                cursor = {'time':last['updated_at'],'id':last['id'],'upper':bound,'paging':True}
            else:
                cursor = {'time':bound,'id':'0' if SOURCES[source][3] else '', 'paging':False}
            output['sources'][source] = {'items':items,'cursor':cursor,'has_more':has_more}
        except Exception as exc:
            output['sources'][source] = {'error':str(exc)}
    return output


if __name__ == '__main__':
    import base64
    import sys
    try:
        print(json.dumps(collect(json.loads(base64.b64decode(sys.argv[1]))),ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({'fatal_error':str(exc)},ensure_ascii=False))
        sys.exit(1)
