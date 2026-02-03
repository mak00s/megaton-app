# Shibuya Analysis プロジェクト

GA4 / Search Console / BigQuery のデータを API で取得し、加工・集計・保存・可視化・分析するプロジェクト。

## 方針

### 技術スタック
- **データ取得**: [megaton](https://github.com/mak00s/megaton) ライブラリ
- **UI**: Jupyter Notebook（ビジネスロジックに近い最低限のコードのみセルに記述）
- **Notebook 同期**: Jupytext（.ipynb ↔ .py 同期）
- **設定管理**: Google Sheets から分析ごとの可変設定をロード
- **言語**: Python 3.8+
- **認証**: サービスアカウント JSON

### 設計原則
1. Notebook はシンプルに保ち、複雑なロジックは別ファイルに分離
2. 再利用可能な処理はユーティリティモジュールとして管理
3. 引き継ぎ事項はこのドキュメントに常に最新化
4. **Git 管理**: 認証情報 JSON 以外は全て Git で管理
5. **Jupytext 運用**: AI Agent は .py ファイルを編集し、編集後に同期コマンドを実行

## ディレクトリ構成

```
shibuya-analysis/
├── PROJECT.md              # 本ドキュメント（方針・予定・進捗・課題）
├── credentials/            # 認証情報（Git 管理外）
│   └── *.json
├── notebooks/              # Jupyter Notebook + 同期された .py
│   ├── 01_ga4_basic.ipynb  # GA4 基本データ取得サンプル
│   └── 01_ga4_basic.py     # ↑ と Jupytext で同期
├── data/                   # 出力データ（CSV等）
├── src/                    # 共通ユーティリティ（必要に応じて）
├── .env                    # 環境変数（Git 管理外）
├── .gitignore              # Git 除外設定
├── jupytext.toml           # Jupytext 設定
└── requirements.txt        # 依存ライブラリ
```

## 予定・TODO

### Phase 1: 環境構築（完了）
- [x] プロジェクトドキュメント作成
- [x] ディレクトリ構成作成
- [x] 依存ライブラリ設定
- [x] .gitignore / .env 設定
- [x] GA4 基本データ取得 Notebook 作成

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

## 運用ルール

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

## 課題・メモ

### 認証情報
- サービスアカウント JSON: `credentials/sa-shibuya-kyousei.json`
- 環境変数 `MEGATON_CREDS_JSON` にパスを設定して使用

### 注意点
- credentials/ フォルダは Git 管理外にすること
- .env ファイルも Git 管理外
- Notebook 実行前に GA4 プロパティの選択が必要

## リファレンス

- [megaton GitHub](https://github.com/mak00s/megaton)
- [megaton チートシート](https://github.com/mak00s/megaton/blob/main/CHEATSHEET.md)
