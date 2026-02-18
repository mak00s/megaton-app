# 進捗履歴

## プロジェクトマイルストーン

### Phase 1: プロジェクト立ち上げ（2026-02-03）
**目標**: ドキュメント整備と基本構成の確立

- ✅ プロジェクト開始、ドキュメント・構成作成
- ✅ Jupytext 設定追加、運用ルール策定
- ✅ Git 初期化、初回コミット完了
- ✅ 認証チェック機能追加、env廃止、設定直書き方式に統一

### Phase 2: 基本機能実装（2026-02-04 〜 2026-02-06）
**目標**: 3つのインターフェース（Notebook/CLI/UI）の構築

- ✅ CLIスクリプト追加（query_ga4.py, query_gsc.py）
- ✅ Gradio UI 構築（JSONパラメータ実行、テーブル/チャート表示）
- ✅ UI機能強化（プロパティ選択、日付入力、JSON⇔UI同期、CSV保存）
- ✅ AGENTS.md をファイル分割（docs/USAGE.md, docs/PROGRESS.md, docs/REFERENCE.md）
- ✅ Gradio → Streamlit に移行
- ✅ BigQuery対応追加（Streamlit UI）
- ✅ 取得件数上限を10万に拡張
- ✅ Notebook: 認証チェックセル削除、セル番号整理
- ✅ UI改善: JSON自動反映、自動実行トグルを縦並びに

### Phase 3: CLI統合とジョブ管理（2026-02-07）
**目標**: 統合CLIによる運用効率化

#### CLI統合の実装

```
Before: 3つの個別スクリプト          After: 統合スクリプト
├── query_ga4.py                  └── query.py
├── query_gsc.py                      ├── --params で自動分岐
└── query_bq.py                       └── 統一スキーマ対応
```

- ✅ CLIを統合し `scripts/query.py` を追加（`--params` で source 自動分岐）
- ✅ `query_ga4.py` / `query_gsc.py` / `query_bq.py` を廃止
- ✅ Streamlit/CLIで共通スキーマ（`schema_version: "1.0"`）運用に統一

#### ジョブ管理機能の追加

```
ジョブライフサイクル:
投入 → 待機 → 実行中 → 完了/失敗
  │      │       │         │
  │      │       └─キャンセル可
  └──────┴────────────────┘
       ジョブ一覧で確認
```

- ✅ `scripts/query.py` にジョブ管理を追加（`--submit`, `--status`, `--result`, `--list-jobs`）
- ✅ `--result` に部分読み込みを追加（`--head N`, `--summary`）
- ✅ `--json` 時の出力を成功/失敗ともに構造化JSONへ統一（`status`, `error_code` など）
- ✅ ジョブキャンセルを追加（`--cancel <job_id>`）

#### 結果パイプライン機能

```
クエリ実行 → フィルタ → 変換 → 集計 → ソート → 出力
              ↓         ↓       ↓       ↓
           --where  --transform --group-by --sort
```

- ✅ `--result` に結果パイプラインを追加（`--where`, `--sort`, `--columns`, `--group-by`, `--aggregate`）
- ✅ 同期実行（`--params`）にも同じ結果パイプラインを適用可能に拡張

## 詳細な変更履歴

## 2026-02-07

- CLIを統合し `scripts/query.py` を追加（`--params` で source 自動分岐）
- `query_ga4.py` / `query_gsc.py` / `query_bq.py` を廃止
- Streamlit/CLIで共通スキーマ（`schema_version: "1.0"`）運用に統一
- `scripts/query.py` にジョブ管理を追加（`--submit`, `--status`, `--result`, `--list-jobs`）
- `--result` に部分読み込みを追加（`--head N`, `--summary`）
- `--json` 時の出力を成功/失敗ともに構造化JSONへ統一（`status`, `error_code` など）
- ジョブキャンセルを追加（`--cancel <job_id>`）
- `--result` に結果パイプラインを追加（`--where`, `--sort`, `--columns`, `--group-by`, `--aggregate`）
- 同期実行（`--params`）にも同じ結果パイプラインを適用可能に拡張

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
