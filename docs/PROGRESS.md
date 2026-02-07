# 進捗履歴

## 2026-02-07

- CLIを統合し `scripts/query.py` を追加（`--params` で source 自動分岐）
- `query_ga4.py` / `query_gsc.py` / `query_bq.py` を廃止
- Streamlit/CLIで共通スキーマ（`schema_version: "1.0"`）運用に統一
- `scripts/query.py` にジョブ管理を追加（`--submit`, `--status`, `--result`, `--list-jobs`）
- `--result` に部分読み込みを追加（`--head N`, `--summary`）
- `--json` 時の出力を成功/失敗ともに構造化JSONへ統一（`status`, `error_code` など）

## 2026-02-06

- Gradio → Streamlit に移行
- BigQuery対応追加（Streamlit UI）
- 取得件数上限を10万に拡張
- Notebook: 認証チェックセル削除、セル番号整理
- UI改善: JSON自動反映、自動実行トグルを縦並びに

## 2026-02-04

- Gradio UI 構築（JSONパラメータ実行、テーブル/チャート表示）
- CLIスクリプト追加（query_ga4.py, query_gsc.py）
- UI機能強化（プロパティ選択、日付入力、JSON⇔UI同期、CSV保存）
- AGENTS.md をファイル分割（docs/USAGE.md, docs/PROGRESS.md, docs/REFERENCE.md）

## 2026-02-03

- プロジェクト開始、ドキュメント・構成作成
- Jupytext 設定追加、運用ルール策定
- Git 初期化、初回コミット完了
- 認証チェック機能追加、env廃止、設定直書き方式に統一
