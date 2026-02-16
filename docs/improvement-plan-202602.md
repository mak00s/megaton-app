# megaton エコシステム 課題と改善案 (2026-02)

SLQM 70th コンテンツ分析（2/4公開後の流入・回遊・定着）を megaton-app 経由で実施した際の振り返り。

## 3つのリポジトリの位置付け

| リポジトリ | 性質 | 対象ユーザー |
|---|---|---|
| **megaton** (PyPI) | 汎用APIラッパー。GA4/GSC/Sheets/BQのfluent API | 自分＋将来の外部利用者 |
| **megaton-app** (megaton_lib) | 案件固有のビジネスロジック。SLQM/Talks/DEI/WITH | 自分（＋CI） |
| **megaton-notebooks** | 実行単位。月次レポートの薄いオーケストレーション | 自分（＋CI） |

---

## A. 今回の分析で実際にぶつかった課題

### 1. アドホック分析がやりにくい

今回のような「70thの流入と回遊を多角的に調べる」作業では、`slqm.py` ノートブックの定型クエリは使えず、毎回 `mg.report.run()` を生で叩いた。megaton_lib にはSLQM用の高レベル分析関数がほぼない（`slqm_ga4.py` は `ym_from_year_month` や `compute_sp_ratio` など低レベルユーティリティのみ）。

→ **ノートブックは「月次定型レポート」に最適化されていて、探索的分析の再利用性が低い**

### 2. BQコホート分析への到達が遠い

GA4 APIでは再訪問コホートが取れず、BQに切り替えた。しかし：
- `megaton_client.py` の `query_bq()` はパラメータ化クエリ非対応
- `talks_retention.py` の `init_bq_client()` は Talks 専用の設計（subsite, table_pv 等がハードコード）
- 結局 `init_bq_client` を流用して生SQLを書いた

→ **BQの汎用的なパラメータ化クエリ実行が megaton_client に欠けている**

### 3. GSCの認証経路が分断

70thの検索クエリを調べようとして `get_gsc('https://corp.shiseido.com/')` → 認証エラー。megatonのサービスアカウントにはSLQMのGSC権限がなく、MCPサーバー（OAuth）でしか取れなかった。

→ **同じデータソースに対して megaton と MCP で認証経路が異なり、使い分けが必要になる**

### 4. 滞在時間メトリクスの罠

`userEngagementDuration` をイベントスコープで取ったら全部0。セッションスコープの `averageSessionDuration` に切り替えたら取れたが、どのメトリクスがどのスコープで動くか、megaton側にガードがない。

→ **megatonがスコープ不一致を検知・警告する仕組みがない**

### 5. `mg.report.data` の暗黙的な状態共有

`mg.report.run()` の結果は `mg.report.data` に格納される。複数クエリを連続実行すると前の結果が上書きされる。今回は毎回 `df_xxx = mg.report.data` で即座に退避したが、うっかり忘れるとデータが消える。

→ **ステートフルな設計がアドホック分析で事故を起こしやすい**

---

## B. コード調査で見えた構造的な課題

### 6. megaton_lib の「汎用」と「案件固有」が混在

| 汎用（再利用可能） | 案件固有 |
|---|---|
| `date_utils`, `date_template`, `sheets`, `credentials`, `megaton_client` | `talks_*.py`, `slqm_ga4.py`, `dei_ga4.py`, `with_report.py` |

汎用モジュールと案件固有モジュールが同じ `megaton_lib/` に同居している。将来的にSLQMが終了した場合、案件固有コードの整理が煩雑になる。

### 7. BQクレデンシャルの二重経路

- `megaton_client.py`: `start.Megaton(creds_path)` → Megaton内部でBQクライアント生成
- `talks_retention.py`: `GOOGLE_APPLICATION_CREDENTIALS` 環境変数 → 直接 `bigquery.Client()`

同じ認証情報を別の方法で初期化しており、どちらのパスが使われるかが暗黙的。

### 8. megaton PyPIパッケージ自体の課題

- **ネストが深い**: `Megaton > Report > Run > ranges()` で4階層。内部で `parent.parent.parent` の参照チェーン
- **フィルタ構文が3種類**: GA4は `field==value`、GSCは `field=~regex`、BQはSQL。統一されていない
- **ReportResult/SearchResult が mutable**: `.df` を直接変更できてしまい、ラッパーの状態と乖離する

---

## C. 改善案（優先度順）

### 高：今回の分析を楽にするもの

#### ① `megaton_client.py` に `query_bq_parameterized()` を追加

```python
def query_bq(project_id, sql, params=None, location="asia-northeast1"):
```

talks_retention の `_run_parameterized` を汎用化。コホート分析などのアドホックBQクエリが megaton_client 経由で統一的に実行できるようになる。

#### ② `mg.report.run()` が DataFrame を直接返すオプション

現状: `mg.report.run(..., show=False)` → `df = mg.report.data`（2行）
改善: `df = mg.report.run(..., show=False).df` または `df = mg.report.run(...).to_df()`

→ 状態退避の忘れを防ぐ。（megaton PyPI側の変更が必要）

#### ③ SLQM用の分析ヘルパー関数群を megaton_lib に追加

`talks_ga4.py` と同レベルの `slqm_analysis.py` を作り、今回書いた定型パターンを関数化：
- `fetch_daily_metrics(mg, date_range, page_pattern)` → 日別UU/PV
- `fetch_page_metrics(mg, date_range, page_pattern)` → ページ別指標+読了率
- `fetch_channel_breakdown(mg, date_range, landing_pattern)` → チャネル別流入

### 中：構造を整理するもの

#### ④ megaton_lib のディレクトリ分割

```
megaton_lib/
  core/          # date_utils, date_template, sheets, credentials, megaton_client
  projects/
    talks/       # talks_*.py
    slqm/        # slqm_ga4.py, slqm_analysis.py
    dei/         # dei_ga4.py
    with_/       # with_report.py
```

汎用と案件固有の境界を明確にする。

#### ⑤ GSC認証の統合

MCPサーバーでしか取れないGSCサイトがある問題。megatonのサービスアカウントにGSC権限を追加するか、megaton_client にOAuth経路を追加する。

### 低：長期的な品質向上

#### ⑥ megaton PyPIのステート管理改善

`mg.report.data` の暗黙上書きを防ぐため、`run()` が毎回新しい Result オブジェクトを返し、内部状態を変更しない設計に。（破壊的変更になるので慎重に）

#### ⑦ メトリクス/ディメンションのスコープ検証

GA4 APIのメタデータを使い、イベントスコープのクエリにセッションスコープのメトリクスを混ぜた場合に警告を出す。

---

## 背景: 今回の分析で行ったこと

SLQM セクション (`corp.shiseido.com/slqm/`) の 2/4 新コンテンツ (`/70th/`) 公開後の分析:

1. **日別 UU/PV バーチャート** — 2/4 に通常の5倍のスパイク確認
2. **ページ別指標 + 読了率** — history.html 37.2%が最高
3. **流入チャネル比較** — Organic Social (Facebook中心) が12倍増
4. **ソース/メディア別** — facebook/social 434ss が最大
5. **回遊分析** — 70th内の回遊構造 + 70th→トップ→商品の2段階回遊
6. **滞在時間** — 70th着地は29.5min vs 既存21.7min
7. **BQコホート定着分析** — 70th着地新規 2.8% vs 既存ページ着地 4.8%
8. **ソース別ファネル** — 70th着地に限定。読売QR (再訪問7.9%, ショップ到達26.3%) が突出
9. **GSC検索クエリ** — 70th配下はクリック4件のみ、SEO未成熟

使用ツール: megaton (PyPI) + megaton_lib + BQ直接クエリ + GSC MCP サーバー
