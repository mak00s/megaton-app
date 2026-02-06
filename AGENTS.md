# AI分析アプリ

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
├── notebooks/              # Jupyter Notebook
├── lib/                    # 共通モジュール
│   └── megaton_client.py   # megaton ラッパー（CLI/UI共通）
├── scripts/                # CLIスクリプト（AI Agent 用）
│   ├── query_ga4.py        # GA4 データ取得
│   ├── query_gsc.py        # GSC データ取得
│   └── query_bq.py         # BigQuery 実行
├── app/                    # Streamlit UI
│   └── streamlit_app.py
├── input/                  # パラメータ入力（AI Agent → UI）
│   └── params.json
└── output/                 # 実行結果出力先
```

## クイックスタート

### AI Agent がデータ取得する場合

```bash
# GSC検索クエリ
python scripts/query_gsc.py --days 14 --limit 1000

# GA4セッション
python scripts/query_ga4.py --days 7 --filter "sessionDefaultChannelGroup==Organic Search"

# BigQuery
python scripts/query_bq.py --project my-project --sql "SELECT * FROM dataset.table LIMIT 100"
```

### Streamlit UI を使う場合

```bash
streamlit run app/streamlit_app.py
# → http://localhost:8501
```

### AI Agent → Streamlit 連携

AI Agent がパラメータをStreamlit UIに自動反映させる方法：

1. `input/params.json` にパラメータを書き込む
2. Streamlit UIが2秒ごとにファイルを監視、変更を検知して自動反映
3. 「自動実行」ONの場合、クエリも自動実行

```bash
# AI Agent がパラメータを書き込み
cat > input/params.json << 'EOF'
{
  "source": "gsc",
  "site_url": "sc-domain:example.com",
  "date_range": {"start": "2025-01-01", "end": "2025-01-31"},
  "dimensions": ["query", "page"],
  "limit": 500
}
EOF
# → Streamlit UIに自動反映される
```

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
4. **コードの一元化**: 共通ロジックは `lib/megaton_client.py` に集約
   - CLI と Streamlit UI で同じコードを使う
   - 重複を避け、修正漏れを防ぐ
5. **AI Agent は CLI 優先**: 人間の確認が不要な場合は Streamlit UI を介さず `scripts/` を直接実行（高速）
