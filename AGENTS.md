# Shibuya Analysis プロジェクト

GA4 / Search Console / BigQuery のデータを API で取得し、加工・集計・保存・可視化・分析するプロジェクト。

## 方針

### 技術スタック
- **データ取得**: [megaton](https://github.com/mak00s/megaton) ライブラリ
- **UI**: Jupyter Notebook（ビジネスロジックに近い最低限のコードのみセルに記述）
- **Notebook 同期**: Jupytext（.ipynb ↔ .py 同期）
- **設定管理**: Google Sheets から分析ごとの可変設定をロード
- **言語**: Python 3.11+
- **認証**: サービスアカウント JSON

### 設計原則
1. Notebook はシンプルに保ち、複雑なロジックは別ファイルに分離
2. 再利用可能な処理はユーティリティモジュールとして管理
3. **AGENTS.md で引き継ぎ**: 方針・予定・進捗・課題は本ドキュメントに常に最新化
   - `AGENTS.md` は Cursor / Claude Code / VS Code Codex が自動認識するファイル名
   - 新規セッション開始時に AI Agent が自動で読み込むため、引き継ぎが確実
4. **Git 管理**: 認証情報 JSON 以外は全て Git で管理
5. **Jupytext 運用**: AI Agent は .py ファイルを編集し、編集後に同期コマンドを実行

## ディレクトリ構成

```
shibuya-analysis/
├── AGENTS.md               # 本ドキュメント（方針・予定・進捗・課題）
├── credentials/            # 認証情報（Git 管理外）
│   └── *.json
├── notebooks/              # Jupyter Notebook + 同期された .py
│   ├── *.ipynb             # Notebook本体
│   └── *.py                # ↑ と Jupytext で同期
├── scripts/                # 実行スクリプト（直接実行する .py）
├── app/                    # Gradio UI アプリ
│   ├── main.py             # エントリーポイント
│   ├── schemas/            # JSONパラメータスキーマ
│   └── engine/             # 実行エンジン（GA4/GSC/可視化）
├── lib/                    # 共通ユーティリティ（importして使う）
├── data/                   # 出力データ（CSV等）
├── .gitignore              # Git 除外設定
├── jupytext.toml           # Jupytext 設定
└── requirements.txt        # 依存ライブラリ
```

## 予定・TODO

### Phase 1: 環境構築（完了）
- [x] プロジェクトドキュメント作成
- [x] ディレクトリ構成作成
- [x] 依存ライブラリ設定（Python 3.11+）
- [x] .gitignore 設定
- [x] GA4 基本データ取得 Notebook 作成
- [x] 認証チェック機能追加

### Phase 2: 基本機能実装
- [ ] GA4 からデータ取得・集計・グラフ表示の一連の流れ確認
- [ ] Search Console データ取得
- [ ] BigQuery データ取得

### Phase 3: 応用・拡張
- [ ] 複数プロパティのバッチ処理
- [ ] Google Sheets への保存
- [ ] 定型レポートの自動化

## 進捗

| 日付 | 内容 |
|------|------|
| 2026-02-03 | プロジェクト開始、ドキュメント・構成作成 |
| 2026-02-03 | Jupytext 設定追加、運用ルール策定 |
| 2026-02-03 | Git 初期化、初回コミット完了 |
| 2026-02-03 | 認証チェック機能追加、env廃止、設定直書き方式に統一 |

## 運用ルール

### Notebook セル構成

```
セル1: 設定・初期化・GA4選択
  - CREDS_PATH, GCP_PROJECT_ID, CONFIG_SHEET_URL を直書き
  - megaton 初期化 → JSON選択UI → GA4選択UI が表示される

セル2: 認証チェック
  - Search Console, Google Sheets, BigQuery の状態を info 表示
  - ✓/✗/- で結果表示、エラーでも続行可能

セル3以降: 本処理
  - データ取得・加工・集計・可視化
```

### Notebook 編集（Jupytext）

AI Agent が Notebook を直接編集すると壊れることがあるため、Jupytext で .py ファイルと同期して運用する。

**編集フロー:**
1. AI Agent は `.py` ファイルを編集
2. 編集後に同期コマンドを実行:
   ```bash
   jupytext --sync notebooks/*.ipynb
   ```
3. Jupyter で `.ipynb` を開いて実行

**手動で .ipynb を編集した場合:**
```bash
jupytext --sync notebooks/*.ipynb
```
これで .py ファイルに変更が反映される。

### スクリプト実行（headlessモード）

Notebook を使わずにスクリプトから megaton を使う場合は、headless モードで初期化する。

```python
from megaton import start

# headless モードで初期化（UIなし）
mg = start.Megaton("credentials/sa-xxx.json", headless=True)

# GA4 アカウント・プロパティを直接指定
mg.ga['4'].account.select("ACCOUNT_ID")
mg.ga['4'].property.select("PROPERTY_ID")

# レポート実行（show=False で表示をスキップ）
mg.report.set.dates(start_date, end_date)
mg.report.run(d=[...], m=[...], show=False)
df = mg.report.data
```

**ポイント:**
- `headless=True`: UI（ipywidgets）を使わない
- `show=False`: レポート実行後の自動表示をスキップ
- アカウント・プロパティは ID を直接指定

### 設定管理（Google Sheets）

分析ごとに可変の設定（対象サイト一覧、フィルタ条件、閾値など）は Google Sheets から読み込む。

**設定シートの例:**
| site_name | ga4_property_id | gsc_site_url | min_impressions |
|-----------|-----------------|--------------|-----------------|
| サイトA   | 123456789       | https://...  | 100             |

**読み込み方法:**
```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
config_df = mg.sheet.to_dataframe()
sites = config_df.to_dict('records')
```

### Gradio UI（対話型分析）

AI Agent と人間が対話しながらデータ分析を行うためのWeb UI。

**起動方法:**
```bash
python app/main.py
# → http://localhost:7860 でアクセス
```

**フロー:**
1. 人間が自然言語で要求（例: 「渋谷サイトの直近7日間のOrganic Search推移」）
2. AI Agent が解釈してJSONパラメータを生成
3. Agent がJSONをGradio UIに貼り付け
4. 人間がUIで確認・修正、実行ボタン押下
5. 結果がテーブル/チャートで表示
6. 人間がOKならAgentが分析を進める

**JSONパラメータ例（GA4）:**
```json
{
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {"start": "2026-01-28", "end": "2026-02-03"},
  "dimensions": ["date"],
  "metrics": ["sessions"],
  "filters": [{"field": "defaultChannelGroup", "op": "==", "value": "Organic Search"}],
  "visualization": {"type": "line", "x": "date", "y": "sessions"}
}
```

## 課題・メモ

### 認証情報
- サービスアカウント JSON: `credentials/` ディレクトリに格納
- Notebook セル1で `CREDS_PATH = "../credentials"` とディレクトリを指定
- ディレクトリ指定により megaton の JSON 選択 UI が表示される

### 注意点
- credentials/ フォルダは Git 管理外
- .env は使用しない（設定は Notebook セル1に直書き）
- Notebook 実行時、セル1で JSON 選択 → GA4 プロパティ選択の順に UI が表示される

## リファレンス

- [megaton GitHub](https://github.com/mak00s/megaton)
- [megaton チートシート](https://github.com/mak00s/megaton/blob/main/CHEATSHEET.md)
