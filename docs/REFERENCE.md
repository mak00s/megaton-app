# 技術リファレンス

## Gradio UI アーキテクチャ

人間とAI Agentが協力してデータ分析を行うフロー：

```
┌─────────────────────────────────────────────────────────────────┐
│                        人間 + AI Agent                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [1] 人間: 自然言語で要求                                        │
│      「渋谷サイトの直近7日間のOrganic Search推移を見たい」         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [2] AI Agent: JSONパラメータ生成                                │
│      {                                                          │
│        "source": "ga4",                                         │
│        "property_id": "254470346",                              │
│        "date_range": {"start": "...", "end": "..."},            │
│        "dimensions": ["date"],                                  │
│        "metrics": ["sessions"],                                 │
│        "filters": [{"field": "defaultChannelGroup", ...}]       │
│      }                                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [3] 人間: Gradio UIでパラメータ確認・修正                        │
│      - JSONを貼り付け → 「↑ UIに読み込み」                        │
│      - ドロップダウンで日付・プロパティ変更                        │
│      - 「↓ JSONに反映」→「実行」                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [4] Gradio UI: 結果表示                                        │
│      - テーブル / チャート（Plotly）                              │
│      - CSV保存 → output/result_*.csv                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  [5] 人間: 結果確認 → OK / 修正依頼                              │
│      OK → AI Agent が output/*.csv を読んで分析続行              │
│      NG → [1] に戻る                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 役割分担

| 役割 | 人間 | AI Agent |
|------|------|----------|
| 要求定義 | 自然言語で指示 | - |
| パラメータ生成 | - | JSONを生成 |
| パラメータ確認 | UIで確認・修正 | - |
| 実行 | ボタン押下 | - |
| 結果確認 | 目視 | - |
| 分析 | 判断 | データ処理・考察 |

---

## JSONパラメータスキーマ

### GA4クエリ

```json
{
  "source": "ga4",
  "property_id": "254470346",
  "date_range": {
    "start": "2026-01-28",
    "end": "2026-02-03"
  },
  "dimensions": ["date"],
  "metrics": ["sessions", "activeUsers"],
  "filters": [
    {"field": "defaultChannelGroup", "op": "==", "value": "Organic Search"}
  ],
  "visualization": {
    "type": "line",
    "x": "date",
    "y": "sessions",
    "title": "セッション推移"
  },
  "limit": 1000
}
```

### GSCクエリ

```json
{
  "source": "gsc",
  "site_url": "https://www.shibuyakyousei.jp/",
  "date_range": {
    "start": "2026-01-21",
    "end": "2026-02-03"
  },
  "dimensions": ["query"],
  "metrics": ["clicks", "impressions", "ctr", "position"],
  "filters": [],
  "visualization": {
    "type": "bar",
    "x": "query",
    "y": "clicks",
    "title": "クエリ別クリック数"
  },
  "limit": 20
}
```

### フィールド説明

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `source` | string | ✓ | `"ga4"` または `"gsc"` |
| `property_id` | string | GA4時 | GA4プロパティID |
| `site_url` | string | GSC時 | Search ConsoleサイトURL |
| `date_range.start` | string | ✓ | 開始日（YYYY-MM-DD） |
| `date_range.end` | string | ✓ | 終了日（YYYY-MM-DD） |
| `dimensions` | array | - | ディメンション一覧 |
| `metrics` | array | - | メトリクス一覧 |
| `filters` | array | - | フィルタ条件 |
| `visualization` | object | - | 可視化設定 |
| `limit` | number | - | 結果件数上限 |

### フィルタ演算子

| 演算子 | 説明 |
|-------|------|
| `==` | 等しい |
| `!=` | 等しくない |
| `>`, `<`, `>=`, `<=` | 比較 |
| `contains` | 部分一致 |
| `not_contains` | 部分不一致 |

### 可視化タイプ

| type | 説明 |
|------|------|
| `table` | テーブルのみ |
| `line` | 折れ線グラフ |
| `bar` | 棒グラフ |
| `pie` | 円グラフ |

---

## 認証情報

### サービスアカウント JSON

- 格納場所: `credentials/` ディレクトリ
- Git管理: **除外**（.gitignoreで設定済み）

### Notebook での指定

```python
CREDS_PATH = "../credentials"  # ディレクトリを指定 → JSON選択UIが表示
```

### スクリプトでの指定

```python
CREDS_PATH = "credentials/sa-shibuya-kyousei.json"  # ファイルを直接指定
```

---

## megaton API

### 初期化

```python
from megaton import start
mg = start.Megaton("credentials/sa-xxx.json", headless=True)
```

### GA4

```python
# アカウント・プロパティ一覧
mg.ga["4"].accounts  # [{"id": "...", "name": "...", "properties": [...]}]

# 選択
mg.ga["4"].account.select("ACCOUNT_ID")
mg.ga["4"].property.select("PROPERTY_ID")

# レポート
mg.report.set.dates("2026-01-01", "2026-01-31")
mg.report.run(d=["date"], m=["sessions"], filters=[...], show=False)
df = mg.report.data
```

#### dimensions / metrics の指定方法

```python
# 基本: 文字列のリスト
d = ["date", "sessionDefaultChannelGroup"]
m = ["sessions", "activeUsers"]

# 列名を変更: タプルで (API名, 表示名) を指定
d = [
    "date",
    ("sessionDefaultChannelGroup", "channel"),  # → 列名が "channel" になる
]
m = [
    ("sessions", "セッション"),      # → 列名が "セッション" になる
    ("activeUsers", "uu"),           # → 列名が "uu" になる
]

mg.report.run(d=d, m=m, show=False)
```

#### フィルタの書式

```python
# フィルタはタプルのリスト: (フィールド名, 演算子, 値)
filters = [
    ("sessionDefaultChannelGroup", "==", "Organic Search"),
    ("country", "==", "Japan"),
]

# 複数フィルタはAND条件
mg.report.run(d=["date"], m=["sessions"], filters=filters, show=False)
```

**演算子一覧:**
| 演算子 | 説明 | 例 |
|-------|------|-----|
| `==` | 完全一致 | `("country", "==", "Japan")` |
| `!=` | 不一致 | `("country", "!=", "Japan")` |
| `contains` | 部分一致 | `("pagePath", "contains", "/blog/")` |
| `not_contains` | 部分不一致 | `("pagePath", "not_contains", "/admin/")` |
| `>`, `<`, `>=`, `<=` | 数値比較 | `("sessions", ">", "100")` |

#### よく使うディメンション・メトリクス

**ディメンション:**
| API名 | 説明 |
|-------|------|
| `date` | 日付 |
| `sessionDefaultChannelGroup` | チャネル（Organic Search等） |
| `sessionSource` | 参照元 |
| `sessionMedium` | メディア |
| `pagePath` | ページパス |
| `landingPage` | ランディングページ |
| `deviceCategory` | デバイス（desktop/mobile/tablet） |
| `country` | 国 |

**メトリクス:**
| API名 | 説明 |
|-------|------|
| `sessions` | セッション数 |
| `activeUsers` | アクティブユーザー数 |
| `newUsers` | 新規ユーザー数 |
| `screenPageViews` | ページビュー数 |
| `bounceRate` | 直帰率 |
| `averageSessionDuration` | 平均セッション時間 |
| `conversions` | コンバージョン数 |

### Search Console

```python
# サイト一覧
sites = mg.search.get.sites()

# 選択
mg.search.use("https://example.com/")

# レポート
mg.search.set.dates("2026-01-01", "2026-01-31")
mg.search.run(dimensions=["query"], metrics=["clicks", "impressions", "ctr", "position"])
df = mg.search.data
```

#### ディメンション・メトリクス

**ディメンション:**
| 名前 | 説明 |
|------|------|
| `query` | 検索クエリ |
| `page` | ページURL |
| `country` | 国 |
| `device` | デバイス（DESKTOP/MOBILE/TABLET） |
| `date` | 日付 |

**メトリクス:**
| 名前 | 説明 |
|------|------|
| `clicks` | クリック数 |
| `impressions` | 表示回数 |
| `ctr` | クリック率（0〜1） |
| `position` | 平均掲載順位 |

### Google Sheets

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
df = mg.sheet.to_dataframe()
```

---

## 外部リンク

- [megaton GitHub](https://github.com/mak00s/megaton)
- [megaton チートシート](https://github.com/mak00s/megaton/blob/main/CHEATSHEET.md)
- [Gradio Documentation](https://www.gradio.app/docs)
- [Plotly Python](https://plotly.com/python/)
