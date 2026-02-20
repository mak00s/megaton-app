# AI分析アプリ

GA4 / Search Console / BigQuery のデータを API で取得し、加工・集計・保存・可視化・分析するプロジェクト。

## アーキテクチャ

| パターン | 用途 | 実行者 | 特徴 |
|----------|------|--------|------|
| **1. Jupyter Notebook** | 探索的分析、可視化、レポート | 人間 / CLI定期実行 | 対話的開発 → そのまま自動化 |
| **2. CLIスクリプト** | データ抽出、バッチ処理 | AI Agent / 人間 | 高速、自動化向き |
| **3. Streamlit UI** | 対話型分析 | 人間 + AI Agent | パラメータ確認・修正 |

```
                 GA4 / GSC / BQ
                       │
                megaton ライブラリ
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
  Notebook          CLI              Streamlit
  .py(jupytext)   query.py           :8501
  分析/レポート    バッチ/Agent        対話型
```

## ディレクトリ構成

```
megaton-app/
├── megaton_lib/        # 共有ライブラリ（pip install -e で他リポジトリから利用）
│   ├── megaton_client.py   # GA4/GSC/BQ 初期化・クエリ実行
│   ├── credentials.py      # サービスアカウント自動検出
│   ├── ga4_helpers.py      # GA4共通ヘルパー
│   └── ...                 # 案件固有モジュール（slqm_*, talks_*, dei_*, with_*）
├── scripts/            # CLIスクリプト
│   ├── query.py            # 統合クエリ実行（GA4/GSC/BigQuery）
│   └── run_notebook.py     # パラメータ付きノートブック実行
├── app/                # Streamlit UI
├── notebooks/          # Jupyter Notebook（Jupytext .py ↔ .ipynb）
├── credentials/        # サービスアカウント JSON（Git管理外）
├── input/              # AI Agent → UI パラメータ受け渡し
├── output/             # クエリ結果・ジョブ管理
├── configs/            # バッチ実行用 JSON
└── tests/              # pytest テスト
```

## AI Agent の基本操作

### CLI（推奨）

```bash
# params.json で同期実行
python scripts/query.py --params input/params.json

# 非同期ジョブ
python scripts/query.py --submit --params input/params.json
python scripts/query.py --status <job_id>
python scripts/query.py --result <job_id> --head 20

# バッチ実行
python scripts/query.py --batch configs/weekly/

# 一覧取得
python scripts/query.py --list-ga4-properties
python scripts/query.py --list-gsc-sites
```

### Python 直接実行（探索的分析）

```python
from megaton_lib.megaton_client import query_ga4, query_gsc
from megaton_lib.analysis import show, properties, sites

df = query_ga4("254800682", "2025-06-01", "2026-01-31",
               dimensions=["month", "year", "sessionDefaultChannelGroup"],
               metrics=["eventCount"],
               filter_d="eventName==purchase")

show(df)                                    # 先頭20行
show(df, save="output/result.csv")          # 保存＋表示
```

**ルール:**
- `print(df.to_string())` は禁止。常に `show()` を使う
- 大きい結果は `save=` でCSV保存し、contextにはサマリだけ載せる

### Streamlit UI 連携

`input/params.json` に書き込む → UIが自動反映（2秒ごと監視）。
`schema_version` は必須（`"1.0"` 固定）。`source` ごとに定義フィールドのみ許可。

## 設計原則

1. **AGENTS.md で引き継ぎ**: Cursor / Claude Code / VS Code Codex が自動認識
2. **Git 管理**: 認証情報 JSON 以外は全て Git で管理
3. **Jupytext 運用**: AI Agent は .py を編集 → `jupytext --sync` で同期
4. **コードの一元化**: 共通ロジックは `megaton_lib/megaton_client.py` に集約
5. **AI Agent は CLI 優先**: 人間の確認が不要なら Streamlit UI を介さず `scripts/` を直接実行

## 詳細ドキュメント

| ドキュメント | 内容 |
|-------------|------|
| [docs/USAGE.md](docs/USAGE.md) | CLI・Notebook・UI の詳細な使い方 |
| [docs/REFERENCE.md](docs/REFERENCE.md) | JSONスキーマ、megaton API、認証、パイプライン |
| [docs/PROGRESS.md](docs/PROGRESS.md) | 変更履歴 |
