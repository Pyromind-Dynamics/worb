#!/usr/bin/env bash
# 默认地址和 wandb api key。需要换环境时分别设置 WORB_BASE_URL、WORB_API_KEY。
# 依次测试：指标上传、文件上传、指标查询、文件查询、指标删除、文件删除。
# 文件上传读取本脚本所在目录下的全部 csv。执行前如果当前 python3 没有 wandb，会先自动安装。结束后打印每步结果，以及 wandb 客户端实际发出的 API。
set -euo pipefail

BASE_URL="${WORB_BASE_URL:-https://pre-worb-cn-east-1.pyromind-asia.cn}"
API_KEY="${WORB_API_KEY:-G00XSZSZ5FPMQXSWZ1PQG00XSZSZ5FPMQXSWZ1PQ}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export WANDB_BASE_URL="$BASE_URL"
export WANDB_API_KEY="$API_KEY"
export WANDB_MODE=online
export WANDB_SILENT=true
export WANDB_DISABLE_CODE=true
export WANDB_DISABLE_GIT=true
export WANDB_SAVE_CODE=false
export WORB_TEST_DIR="$SCRIPT_DIR"

if ! python3 -c 'import wandb' >/dev/null 2>&1; then
  echo "wandb 未安装，开始安装"
  python3 -m pip install wandb
  python3 -c 'import wandb'
  echo "wandb 安装完成"
fi

exec python3 - "$BASE_URL" "$SCRIPT_DIR" <<'PY'
import glob
import os
import sys
import traceback
from urllib.parse import parse_qsl, urlsplit, urlunsplit, urlencode

base_url = sys.argv[1]
script_dir = sys.argv[2]
api_key = os.environ.get("WANDB_API_KEY", "")
results = []
api_log = []


def redact(text):
    text = str(text)
    if api_key:
        text = text.replace(api_key, "***")
    return text


def record_url(url):
    parts = urlsplit(url)
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in {"apikey", "api_key", "token"}:
            value = "***"
        query.append((key, value))
    shown = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
    return redact(shown)


def install_api_log():
    import requests

    original = requests.Session.request

    def wrapped(self, method, url, *args, **kwargs):
        response = original(self, method, url, *args, **kwargs)
        api_log.append("%s %s %s" % (method.upper(), record_url(url), response.status_code))
        return response

    requests.Session.request = wrapped


def step(name, fn):
    try:
        detail = fn()
        results.append((name, True, detail or ""))
        print("[PASS] %s %s" % (name, detail or ""))
    except Exception as exc:
        detail = redact("%s: %s" % (type(exc).__name__, exc))
        results.append((name, False, detail))
        print("[FAIL] %s %s" % (name, detail))
        print(redact(traceback.format_exc()))


def csv_paths():
    found = []
    for pattern in ("*.csv", "*.CSV"):
        found.extend(glob.glob(os.path.join(script_dir, pattern)))
    return sorted(set(found))


def graphql(api, query, variables):
    from wandb_gql import gql

    client = getattr(api, "client", None)
    if client is None or not hasattr(client, "execute"):
        raise RuntimeError("wandb.Api 没有 graphql client")
    # wandb 的 execute 只接受语法树。传入字符串会在客户端报 Invalid AST Node，请求到不了服务端。
    if isinstance(query, str):
        query = gql(query)
    payload = client.execute(query, variable_values=variables)
    if isinstance(payload, dict) and "project" not in payload and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def load_run(api, entity, project, run_id):
    path = "%s/%s/%s" % (entity, project, run_id)
    return api.run(path)


RUN_QUERY = """
query($entity: String!, $project: String!, $name: String!) {
  project(name: $project, entityName: $entity) {
    run(name: $name) {
      historyLineCount
      historyKeys
      summaryMetrics
      historyTail
      files { edges { node { name id url } } }
    }
  }
}
"""


def main():
    try:
        import wandb
    except ImportError:
        print("[FAIL] 未安装 wandb。请先执行 pip install wandb")
        return 1
    install_api_log()
    print("base_url=%s" % base_url)
    print("script_dir=%s" % script_dir)

    holder = {}

    def upload_metrics():
        run = wandb.init(project="worb-client-test", job_type="client-test", reinit=True)
        holder["run"] = run
        for step_id in range(3):
            run.log({"test/loss": 1.0 - step_id * 0.1, "test/accuracy": step_id * 0.2}, step=step_id)
        holder["entity"] = run.entity
        holder["project"] = run.project
        holder["run_id"] = run.id
        return "entity=%s project=%s run=%s steps=3" % (run.entity, run.project, run.id)

    def upload_files():
        paths = csv_paths()
        if not paths:
            raise RuntimeError("脚本目录没有 csv 文件")
        run = holder["run"]
        names = []
        for path in paths:
            run.save(path, base_path=script_dir, policy="now")
            names.append(os.path.basename(path))
        holder["files"] = names
        run.finish()
        holder["finished"] = True
        return "files=%s" % ",".join(names)

    def query_metrics():
        api = wandb.Api()
        holder["api"] = api
        data = graphql(api, RUN_QUERY, {
            "entity": holder["entity"],
            "project": holder["project"],
            "name": holder["run_id"],
        })
        run = (((data or {}).get("project") or {}).get("run")) or {}
        holder["remote"] = run
        count = run.get("historyLineCount")
        summary = run.get("summaryMetrics") or ""
        if not count and "test/loss" not in summary and not run.get("historyKeys") and not run.get("historyTail"):
            raise RuntimeError("没有读到指标 historyLineCount=%s" % count)
        return "historyLineCount=%s" % count

    def query_files():
        run = holder.get("remote") or {}
        edges = ((run.get("files") or {}).get("edges")) or []
        names = [((edge.get("node") or {}).get("name")) for edge in edges]
        names = [name for name in names if name]
        holder["remote_files"] = names
        missing = [name for name in holder.get("files") or [] if name not in names]
        if missing:
            raise RuntimeError("未查到文件 %s，实际=%s" % (",".join(missing), ",".join(names)))
        return "files=%s" % ",".join(names)

    def delete_one(api, name):
        remote = load_run(api, holder["entity"], holder["project"], holder["run_id"])
        target = None
        # 按文件名查询。不带 names 时客户端会把这个 run 的文件逐页拉完。
        for item in remote.files(names=[name]):
            target = item
            break
        if target is None:
            raise RuntimeError("远端没有文件 %s" % name)
        target.delete()

    def delete_metrics():
        delete_one(holder["api"], "wandb-history.jsonl")
        return "deleted=wandb-history.jsonl"

    def delete_files():
        names = holder.get("files") or []
        if not names:
            raise RuntimeError("没有可删除的 csv")
        for name in names:
            delete_one(holder["api"], name)
        return "deleted=%s" % ",".join(names)

    step("指标上传", upload_metrics)
    if "run" in holder:
        step("文件上传", upload_files)
        if not holder.get("finished"):
            try:
                holder["run"].finish()
            except Exception as exc:
                print("[FAIL] 结束 run %s" % redact(exc))
                print(redact(traceback.format_exc()))
        if holder.get("finished") or holder.get("run_id"):
            step("指标查询", query_metrics)
            step("文件查询", query_files)
            if "api" in holder:
                step("指标删除", delete_metrics)
                step("文件删除", delete_files)
    else:
        print("[FAIL] 指标上传未创建 run，后续步骤未执行")

    print("")
    print("wandb api 记录 (%d)" % len(api_log))
    for line in api_log:
        print(line)
    failed = [name for name, ok, _ in results if not ok]
    print("")
    print("结果 passed=%d failed=%d" % (len(results) - len(failed), len(failed)))
    return 1 if failed or len(results) < 6 else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
PY
