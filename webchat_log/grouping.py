"""只推导展示信息，不改写采集到的原始状态。"""
import re

AUXILIARY = {'标题', '开场提示', '追问建议', '辅助任务'}
SUCCESS = {'completed', 'success', 'succeeded'}
RETRY = re.compile(r'^(req_.+)-r([1-9][0-9]*)$')
CONTINUATION = re.compile(r'^(req_.+)-fn-([1-9][0-9]*)$')
SIDECAR = re.compile(r'^(req_.+)-(web|img)-call_.+$')


def describe(item):
    request = item.get('request_id') or ''
    metadata = item.get('metadata') or {}
    managed = metadata.get('managed_function_calling') or {}
    kind = item.get('auxiliary_kind') or metadata.get('auxiliary_kind')
    parent = item.get('parent_request_id') or metadata.get('parent_request_id') or managed.get('parent_request_id') or item.get('trigger_request_id') or metadata.get('trigger_request_id')
    source = item['source']
    label = {'gateway': '关联网关错误', 'message': '消息执行', 'tool': '工具执行', 'audio': '音频转写'}.get(source, source)
    stage, retry = 0, 0
    if source == 'webchat':
        attempt = RETRY.fullmatch(request)
        base = attempt[1] if attempt else request
        retry = int(attempt[2]) if attempt else 0
        continuation = CONTINUATION.fullmatch(base)
        sidecar = SIDECAR.fullmatch(base)
        label = {'title_generation': '标题', 'spark_prelude': '开场提示', 'follow_up_generation': '追问建议'}.get(kind or "")
        if not label:
            label = ('标题' if request.startswith('req_title_') else '辅助任务' if request.startswith('req_aux_') else
                     ('搜索子请求' if sidecar[2] == 'web' else '图片子请求') if sidecar else
                     '工具续答' if continuation or str(managed.get('continuation_items') or item.get('continuation_items') or '0').isdigit() and int(managed.get('continuation_items') or item.get('continuation_items') or 0) > 0 else
                     '主对话重试' if attempt else '主对话')
        if continuation:
            stage = int(continuation[2])
        if not parent:
            if request.startswith('req_title_'): parent = 'req_' + request[len('req_title_'):]
            elif request.startswith('req_aux_aux:spark_prelude:'): parent = request.rsplit(':', 1)[-1]
            elif attempt: parent = base
            elif sidecar: parent = sidecar[1]
            elif continuation: parent = continuation[1]
    item.update(task_type=label, parent_request_id=parent or None, execution_stage=stage, retry_index=retry)
    return item


def fallback_parent(request):
    """中间记录未采集时，仅使用完整请求编号的结构关系。"""
    for pattern in (RETRY, SIDECAR, CONTINUATION):
        match = pattern.fullmatch(request or '')
        if match: return match[1]
    return None


def summarize(group):
    members = [group] + group['children']
    group['group_status'] = group.get('status', 'unknown')
    if group['task_type'] != '主对话':
        group['outcome'] = '未关联主请求 · ' + ('失败' if group.get('is_error') else group.get('status', '未知'))
        return
    main = [r for r in members if r['task_type'] in {'主对话', '主对话重试', '工具续答'}]
    latest = max(main, key=lambda r: (r['execution_stage'], r['retry_index'], r.get('created_at', ''), r['key']))
    status = 'failed' if latest.get('is_error') else latest.get('status', 'unknown')
    group['group_status'] = status
    group['outcome_request_id'] = latest.get('request_id')
    failures = [r for r in members if r.get('is_error')]
    if status == 'failed':
        group['outcome'] = '主任务失败（工具续答失败）' if latest['task_type'] == '工具续答' else '主任务失败'
    elif status in SUCCESS:
        group['outcome'] = '主任务成功'
        if any(r.get('is_error') for r in main): group['outcome'] += '，重试后恢复'
        if any(r['task_type'] not in AUXILIARY | {'主对话', '主对话重试', '工具续答', '消息执行'} for r in failures): group['outcome'] += '，子任务失败'
        if any(r['task_type'] in AUXILIARY for r in failures): group['outcome'] += '，辅助任务失败'
    else:
        group['outcome'] = {'running': '主任务执行中', 'created': '主任务等待中', 'pending': '主任务等待中', 'cancelled': '主任务已取消', 'settlement_pending': '主任务待结算'}.get(status, '主任务状态未知')
    group['group_is_error'] = status == 'failed'
