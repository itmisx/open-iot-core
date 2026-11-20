# Python 示例

boto3。

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt

export OIOT_ENDPOINT=http://127.0.0.1:9090/api/aws
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=...
venv/bin/python main.py
```

教程对应页：控制台里的 `#/guide/sdk/python`。`main.py` 里的
`tutorial:begin` / `tutorial:end` 标记就是那一页引用的锚点，别随手删。
