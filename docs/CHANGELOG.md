# 変更履歴

機能追加・不具合修正・仕様変更など利用者影響のある変更のみ記載し、軽微な文言修正は記載しない。

## 変更履歴

### 2026-02-16

- 分析振り返りに基づく改善施策を一括実施
  - `query_bq()` パラメータ化クエリ対応、`get_bq_client()` 新設
  - `query_ga4()` / 分析ヘルパー側 `_run()` で `result.df` を直接返すように修正
  - 分析ヘルパー関数を8件追加
  - `ga4_helpers.py` 新設（共通GA4ヘルパー抽出）
  - GSC認証統合（MCP側 JSON を credentials/ に symlink）
  - 公開API入力バリデーション追加
  - バッチエラーレポート構造化
  - テスト基盤整備（pytest マーカー `unit`/`contract`/`integration`）
  - 全479テスト pass

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

- プロジェクト開始、構成作成
- Git 初期化、認証チェック機能追加

## 今後検討メモ（2026-02時点）

- `megaton_lib` のディレクトリ分割（汎用モジュールと案件固有モジュールの境界整理）
- `megaton` PyPI 側のステート管理改善（`run()` の非ステートフル化）
- GA4 メトリクス/ディメンションのスコープ不一致の自動検証（現状は docstring 注意喚起で対応）
