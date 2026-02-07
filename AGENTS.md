# AI分析アプリ

GA4 / Search Console / BigQuery のデータを API で取得し、加工・集計・保存・可視化・分析するプロジェクト。

## アーキテクチャ

本プロジェクトは**用途に応じた3つのデータ取得方法**を提供する。

| パターン | 用途 | 実行者 | 特徴 |
|----------|------|--------|------|
| **1. Jupyter Notebook** | 探索的分析、可視化、レポート | 人間 / CLI定期実行 | 対話的開発 → そのまま自動化 |
| **2. CLIスクリプト** | データ抽出、バッチ処理 | AI Agent / 人間 | 高速、自動化向き |
| **3. Streamlit UI** | 対話型分析 | 人間 + AI Agent | パラメータ確認・修正 |

```
┌─────────────────────────────────────────────────────────────┐
│                      データソース                            │
│         GA4 / Search Console / BigQuery                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    megaton ライブラリ
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ 1. Notebook   │  │ 2. CLI        │  │ 3. Streamlit  │
│ (分析/レポート)│  │ (バッチ/Agent)│  │ (対話型分析)  │
│               │  │               │  │               │
│ notebooks/    │  │ scripts/      │  │ app/          │
│ *.py (jupytext)│ │ query.py      │  │ localhost:8501│
│               │  │ run_notebook  │  │               │
└──────┬────────┘  └───────────────┘  └───────────────┘
       │
       └─→ scripts/run_notebook.py でCLI/CI実行も可
```

### Streamlit UIを使う理由

AI Agent と人間が協力してデータ分析を行う際の課題を解決：
- **Agent単独**: パラメータの妥当性を人間が確認できない
- **人間単独**: SQLやAPIパラメータを書くのが面倒

**解決策**: Streamlit UIで役割分担
1. 人間が自然言語で要求 → AI Agent がJSONパラメータ生成
2. 人間がUIでパラメータ確認・修正 → 実行
3. 結果を人間が目視確認 → OKならAgentが分析続行

**Streamlitを選んだ理由:**
- データ分析ダッシュボード向けに設計
- サイドバーでパラメータ、メインで結果という自然な構成
- キャッシュ機能で再実行が高速

## 技術スタック

- **データ取得**: [megaton](https://github.com/mak00s/megaton) v0.8.1+
- **Notebook**: Jupyter + Jupytext
- **CLI**: Python スクリプト（headlessモード）
- **UI**: Streamlit
- **言語**: Python 3.11+
- **認証**: サービスアカウント JSON（`credentials/` に配置）

## ディレクトリ構成

```
shibuya-analysis/
├── AGENTS.md               # 本ドキュメント（AI Agent が最初に読む）
├── docs/                   # 詳細ドキュメント
│   ├── USAGE.md            # 使い方ガイド
│   ├── PROGRESS.md         # 進捗履歴
│   └── REFERENCE.md        # 技術リファレンス
├── credentials/            # 認証情報（Git管理外）
├── notebooks/              # Jupyter Notebook（Jupytext .py ↔ .ipynb）
│   └── reports/            # 分析レポート用ノートブック
├── lib/                    # 共通モジュール
│   ├── megaton_client.py   # megaton ラッパー（CLI/UI共通）
│   └── notebook.py         # ノートブック初期化ヘルパー（init()）
├── scripts/                # CLIスクリプト（AI Agent 用）
│   ├── query.py            # 統合クエリ実行（GA4/GSC/BigQuery）
│   └── run_notebook.py     # ノートブック実行（パラメータ上書き対応）
├── app/                    # Streamlit UI
│   └── streamlit_app.py
├── input/                  # パラメータ入力（AI Agent → UI）
│   └── params.json
└── output/                 # 実行結果出力先
```

## クイックスタート

### AI Agent がデータ取得する場合

```bash
# 共通 params.json（厳格スキーマ）で実行
python scripts/query.py --params input/params.json

# 非同期ジョブとして投入
python scripts/query.py --submit --params input/params.json

# ジョブ状態確認
python scripts/query.py --status job_20260207_120000_abcd1234

# ジョブをキャンセル
python scripts/query.py --cancel job_20260207_120000_abcd1234

# ジョブ結果確認
python scripts/query.py --result job_20260207_120000_abcd1234

# 先頭N行だけ取得
python scripts/query.py --result job_20260207_120000_abcd1234 --head 20

# 要約統計だけ取得
python scripts/query.py --result job_20260207_120000_abcd1234 --summary

# 結果の変換
python scripts/query.py --result job_20260207_120000_abcd1234 --json --transform "date:date_format"
python scripts/query.py --result job_20260207_120000_abcd1234 --json --transform "page:url_decode,page:strip_qs,page:path_only"
python scripts/query.py --result job_20260207_120000_abcd1234 --json --transform "page:strip_qs:id,ref" --group-by "page" --aggregate "sum:clicks"

# 結果のフィルタ・集計（--result はCLI引数で指定）
python scripts/query.py --result job_20260207_120000_abcd1234 --json --where "clicks > 10" --sort "clicks DESC" --head 20
python scripts/query.py --result job_20260207_120000_abcd1234 --json --group-by "page" --aggregate "sum:clicks,mean:ctr" --sort "sum_clicks DESC"
python scripts/query.py --result job_20260207_120000_abcd1234 --json --columns "query,clicks" --where "impressions >= 100"

# パイプライン付き同期クエリ（params.json に pipeline を含める）
# 例: input/params.json に以下を記述
# {
#   "schema_version": "1.0",
#   "source": "gsc",
#   "site_url": "https://www.example.com/",
#   "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
#   "dimensions": ["query", "page"],
#   "limit": 25000,
#   "pipeline": {
#     "transform": "page:url_decode,page:strip_qs,page:path_only",
#     "group_by": "page",
#     "aggregate": "sum:clicks,sum:impressions",
#     "sort": "sum_clicks DESC",
#     "head": 50
#   }
# }
python scripts/query.py --params input/params.json --json

# 保存先指定付きクエリ（params.json に save を含める）
# 例1: CSVに保存
# {
#   "schema_version": "1.0",
#   "source": "gsc",
#   "site_url": "https://www.example.com/",
#   "date_range": {"start": "2026-01-21", "end": "2026-02-03"},
#   "dimensions": ["query"],
#   "save": {"to": "csv", "path": "output/report.csv"}
# }

# 例2: Google Sheetsにアップサート
# {
#   ...
#   "save": {
#     "to": "sheets",
#     "sheet_url": "https://docs.google.com/spreadsheets/d/xxxxx",
#     "sheet_name": "weekly_data",
#     "mode": "upsert",
#     "keys": ["date", "page"]
#   }
# }

# 例3: BigQueryに上書き保存
# {
#   ...
#   "pipeline": {"transform": "page:url_decode,page:path_only", "sort": "clicks DESC"},
#   "save": {
#     "to": "bigquery",
#     "project_id": "my-project",
#     "dataset": "analytics",
#     "table": "gsc_pages",
#     "mode": "overwrite"
#   }
# }
python scripts/query.py --params input/params.json --json

# 一覧取得
python scripts/query.py --list-ga4-properties
python scripts/query.py --list-gsc-sites
python scripts/query.py --list-bq-datasets --project my-project
python scripts/query.py --list-jobs

# バッチ実行（ディレクトリ内のJSONをファイル名順に実行）
python scripts/query.py --batch configs/weekly/
python scripts/query.py --batch configs/weekly/ --json
```

`--json` 指定時は、成功/失敗ともに構造化JSONを返す（失敗時は `status=error`, `error_code` を含む）。

### 日付テンプレート

`date_range` に相対日付式を使うと、同じconfigを毎回書き換えずに再利用できる：

| 式 | 意味 | 例（2026-02-07実行時） |
|----|------|----------------------|
| `today` | 実行日 | 2026-02-07 |
| `today-Nd` | N日前 | today-7d → 2026-01-31 |
| `today+Nd` | N日後 | today+3d → 2026-02-10 |
| `month-start` | 当月1日 | 2026-02-01 |
| `month-end` | 当月末日 | 2026-02-28 |
| `prev-month-start` | 前月1日 | 2026-01-01 |
| `prev-month-end` | 前月末日 | 2026-01-31 |
| `week-start` | 今週月曜日 | 2026-02-02 |
| `YYYY-MM-DD` | 絶対日付 | そのまま |

```json
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "sc-domain:example.com",
  "date_range": {"start": "today-30d", "end": "today-3d"},
  "dimensions": ["query", "page"],
  "limit": 25000
}
```

### バッチ実行

`configs/` ディレクトリにJSONを配置し、`--batch` で一括実行：

```
configs/
  weekly/
    01_gsc_queries.json     ← ファイル名順に実行
    02_ga4_channels.json
  monthly/
    01_ga4_summary.json
```

```bash
python scripts/query.py --batch configs/weekly/
python scripts/query.py --batch configs/monthly/ --json
```

各configは独立した1ステップ。失敗しても残りは続行し、最後にサマリを表示。

### ノートブック実行

Jupytext percent format の `.py` ノートブックを CLI から実行する。
Jupyter で対話的に開発 → 同じファイルを CLI / GitHub Actions で定期実行。

```bash
# デフォルトパラメータで実行
python scripts/run_notebook.py notebooks/reports/yokohama_cv.py

# パラメータ上書き（日付テンプレート対応）
python scripts/run_notebook.py notebooks/reports/yokohama_cv.py \
  -p START_DATE=today-30d -p END_DATE=today

# 複数パラメータ
python scripts/run_notebook.py notebooks/reports/yokohama_cv.py \
  -p START_DATE=2025-01-01 -p END_DATE=2025-01-31 \
  -p OUTPUT_DIR=output/jan
```

ノートブックの `# %% tags=["parameters"]` セルの変数が上書き対象。
`MPLBACKEND=Agg` を設定して実行するため、`plt.show()` は呼ばれてもGUI表示されない（ヘッドレス実行）。

### AI Agent が探索的分析をする場合（Python直接実行）

CLIやStreamlitを介さず、Pythonコードを直接実行して分析する場合は
`lib/analysis` のヘルパーを使う。**context を浪費しない**設計。

```python
from lib.megaton_client import query_ga4, query_gsc
from lib.analysis import show, properties, sites

# プロパティ/サイト一覧
properties()
sites()

# クエリ実行（結果はDataFrame、contextに載らない）
df = query_ga4("254800682", "2025-06-01", "2026-01-31",
               dimensions=["month", "year", "sessionDefaultChannelGroup"],
               metrics=["eventCount"],
               filter_d="eventName==purchase")

# Python加工（contextゼロ）
df = df[df["sessionDefaultChannelGroup"] != "Direct"]

# 表示（行数制限付き、必要ならCSV保存）
show(df)                                    # 先頭20行
show(df, n=10)                              # 先頭10行
show(df, save="output/result.csv")          # 保存＋先頭20行
show(df, n=5, save="output/result.csv")     # 保存＋先頭5行
```

**ルール:**
- `print(df.to_string())` は禁止。常に `show()` を使う
- 大きい結果は `save=` でCSV保存し、contextにはサマリだけ載せる
- 加工・集計はpandas で行い、最終結果だけ `show()` する

### Streamlit UI を使う場合

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501
```

### AI Agent → Streamlit 連携

AI Agent がパラメータをStreamlit UIに自動反映させる方法：

1. `input/params.json` にパラメータを書き込む
2. Streamlit UIが2秒ごとにファイルを監視し、更新時刻 + 実質差分（空白/インデント/キー順は無視）で変更を検知して自動反映
3. 「自動実行」ONの場合、クエリも自動実行

```bash
# AI Agent がパラメータを書き込み
cat > input/params.json << 'EOF'
{
  "schema_version": "1.0",
  "source": "gsc",
  "site_url": "sc-domain:example.com",
  "date_range": {"start": "2025-01-01", "end": "2025-01-31"},
  "dimensions": ["query", "page"],
  "limit": 500
}
EOF
# → Streamlit UIに自動反映される
```

**必須ルール（完全移行）:**
- `schema_version` は必須（現在は `"1.0"` 固定）
- `source` ごとに定義されたフィールドのみ許可（未知フィールドはエラー）

**UIの設定:**
- 「JSON自動反映」: ON/OFFでファイル監視を切り替え
- 「自動実行」: ONにするとパラメータ反映後に自動でクエリ実行

## TODO

### Phase 2: 基本機能実装（完了）
- [x] GA4 データ取得
- [x] Search Console データ取得
- [x] BigQuery データ取得
- [x] CLIスクリプト（GA4/GSC/BigQuery）
- [x] Streamlit UI
- [x] Google Sheets への保存（UI経由）

### Phase 3: 応用・拡張
- [ ] 複数プロパティのバッチ処理
- [x] 定型レポートの自動化（`scripts/run_notebook.py` + GitHub Actions）

## 詳細ドキュメント

| ドキュメント | 内容 |
|-------------|------|
| [docs/USAGE.md](docs/USAGE.md) | Notebook / CLI / UI の使い方 |
| [docs/PROGRESS.md](docs/PROGRESS.md) | 進捗履歴 |
| [docs/REFERENCE.md](docs/REFERENCE.md) | JSONスキーマ、API仕様 |

## 設計原則

1. **AGENTS.md で引き継ぎ**: Cursor / Claude Code / VS Code Codex が自動認識
2. **Git 管理**: 認証情報 JSON 以外は全て Git で管理
3. **Jupytext 運用**: AI Agent は .py を編集 → `jupytext --sync` で同期
4. **コードの一元化**: 共通ロジックは `lib/megaton_client.py` に集約
   - CLI と Streamlit UI で同じコードを使う
   - 重複を避け、修正漏れを防ぐ
5. **AI Agent は CLI 優先**: 人間の確認が不要な場合は Streamlit UI を介さず `scripts/` を直接実行（高速）
