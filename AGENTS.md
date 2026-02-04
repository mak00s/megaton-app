# Shibuya Analysis プロジェクト

GA4 / Search Console / BigQuery のデータを API で取得し、加工・集計・保存・可視化・分析するプロジェクト。

## アーキテクチャ

本プロジェクトは**用途に応じた3つのデータ取得方法**を提供する。

| パターン | 用途 | 実行者 | 特徴 |
|----------|------|--------|------|
| **1. Jupyter Notebook** | 探索的分析、可視化、メモ | 人間 | 対話的、試行錯誤向き |
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
│ (対話的分析)   │  │ (バッチ/Agent)│  │ (対話型分析)  │
│               │  │               │  │               │
│ notebooks/    │  │ scripts/      │  │ app/          │
│ *.ipynb       │  │ query_*.py    │  │ localhost:8501│
└───────────────┘  └───────────────┘  └───────────────┘
```

### Streamlit UIを作った理由

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

- **データ取得**: [megaton](https://github.com/mak00s/megaton)
- **Notebook**: Jupyter + Jupytext
- **CLI**: Python スクリプト（headlessモード）
- **UI**: Streamlit
- **言語**: Python 3.11+
- **認証**: サービスアカウント JSON

## ディレクトリ構成

```
shibuya-analysis/
├── AGENTS.md               # 本ドキュメント
├── docs/                   # 詳細ドキュメント
│   ├── USAGE.md            # 使い方ガイド
│   ├── PROGRESS.md         # 進捗履歴
│   └── REFERENCE.md        # 技術リファレンス
├── credentials/            # 認証情報（Git管理外）
├── notebooks/              # Jupyter Notebook
├── scripts/                # CLIスクリプト
│   ├── query_ga4.py
│   └── query_gsc.py
├── app/                    # Streamlit UI
├── lib/                    # 共通ユーティリティ
└── output/                 # 実行結果出力先
```

## クイックスタート

### AI Agent がデータ取得する場合

```bash
# GSC検索クエリ
python scripts/query_gsc.py --days 14 --limit 20

# GA4セッション
python scripts/query_ga4.py --days 7 --filter "defaultChannelGroup==Organic Search"
```

### Streamlit UI を使う場合

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501
```

## TODO

### Phase 2: 基本機能実装（完了）
- [x] GA4 データ取得
- [x] Search Console データ取得
- [x] CLIスクリプト
- [x] Streamlit UI
- [ ] BigQuery データ取得

### Phase 3: 応用・拡張
- [ ] 複数プロパティのバッチ処理
- [ ] Google Sheets への保存
- [ ] 定型レポートの自動化

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
