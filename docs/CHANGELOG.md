# 変更履歴

機能追加・不具合修正・仕様変更など利用者影響のある変更のみ記載し、軽微な文言修正は記載しない。

### 2026-02-21

- Streamlit UI 多言語対応（日本語・英語）
- フィルタ・集計UIの内部表現を言語非依存に統一
- 翻訳整合性テストを追加

### 2026-02-16

- BQ パラメータ化クエリ対応（`query_bq()` / `get_bq_client()`）
- GA4 実行結果の扱いを `result.df` ベースに統一
- 分析ヘルパー追加と GA4 共通ヘルパー抽出
- 公開API入力バリデーションとバッチエラーレポートを構造化
- テスト基盤を整備（pytest マーカー `unit`/`contract`/`integration`）

### 2026-02-07

- CLIを統合し `scripts/query.py` を追加（`--params` で source 自動分岐）
- `query_ga4.py` / `query_gsc.py` / `query_bq.py` を廃止
- Streamlit/CLIで共通スキーマ（`schema_version: "1.0"`）運用に統一
- ジョブ管理機能追加（`--submit`, `--status`, `--result`, `--cancel`, `--list-jobs`）
- 結果パイプライン追加（`--where`, `--sort`, `--columns`, `--group-by`, `--aggregate`）

### 2026-02-06

- Gradio → Streamlit に移行
- BigQuery対応追加（Streamlit UI）
- 取得件数上限を10万に拡張

### 2026-02-04

- Gradio UI 構築、CLIスクリプト追加

### 2026-02-03

- 認証チェック機能を追加
