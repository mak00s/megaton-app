# megaton エコシステム 課題と改善案 (2026-02)

SLQM 70th コンテンツ分析（2/4公開後の流入・回遊・定着）を megaton-app 経由で実施した際の振り返り。

## 3つのリポジトリの位置付け

| リポジトリ | 性質 | 対象ユーザー |
|---|---|---|
| **megaton** (PyPI) | 汎用APIラッパー。GA4/GSC/Sheets/BQのfluent API | 自分＋将来の外部利用者 |
| **megaton-app** (megaton_lib) | 案件固有のビジネスロジック。SLQM/Talks/DEI/WITH | 自分（＋CI） |
| **megaton-notebooks** | 実行単位。月次レポートの薄いオーケストレーション | 自分（＋CI） |

---

## 課題の整理

### ✅ 解決済みの課題（優先度: 高）

以下の課題は2026-02-16に対応完了し、全462テストがpass。

| # | 課題 | 解決内容 | 対応日 |
|---|------|---------|-------|
| ① | **BQパラメータ化クエリ未対応** | `query_bq()` に `params` 引数追加、`get_bq_client()` 新設 | 2026-02-16 |
| ② | **ステートフルなmg.report.data** | `query_ga4()` と `slqm_analysis._run()` で `result.df` を直接返すように修正 | 2026-02-16 |
| ③ | **SLQM分析ヘルパー不足** | `slqm_analysis.py` に8関数追加（日別指標、チャネル分析等） | 2026-02-16 |
| ⑤ | **GSC認証経路分断** | MCP側の `sa_corp-1872_gsc-api.json` を symlink で統合 | 2026-02-16 |

#### 解決詳細

**① BQコホート分析への到達が遠い → 解決**
- 課題: `query_bq()` はパラメータ化クエリ非対応、`init_bq_client()` は Talks 専用設計
- 解決: `query_bq()` を拡張し `params` 引数でパラメータ化クエリ対応。`get_bq_client()` を新設して汎用化
- 変更ファイル: `megaton_lib/megaton_client.py`, `megaton_lib/talks_retention.py`, `tests/test_megaton_execution.py`
- テスト: 6件追加

**② mg.report.data の暗黙的な状態共有 → 緩和**
- 課題: 複数クエリ連続実行で前の結果が上書き、うっかり忘れるとデータ消失
- 解決: megaton は既に `ReportResult` を返していた。`query_ga4()` と `slqm_analysis._run()` で `result.df` を直接使うように修正
- 変更ファイル: `megaton_lib/megaton_client.py`, `megaton_lib/slqm_analysis.py`
- megaton_lib 内では `mg.report.data` を参照しない設計に統一

**③ アドホック分析がやりにくい → 解決**
- 課題: 定型クエリは使えず、毎回 `mg.report.run()` を生で叩く必要
- 解決: `slqm_analysis.py` に8つのヘルパー関数追加
  - `fetch_daily_metrics`: 日別 UU/sessions/PV
  - `fetch_page_metrics`: ページ別指標 + 読了率
  - `fetch_channel_breakdown`: チャネル別流入
  - `fetch_source_medium`: ソース/メディア別流入
  - `fetch_landing_pages`: ランディングページ別
  - `fetch_session_quality`: 滞在時間・ページ/セッション
  - `fetch_new_vs_returning`: 新規 vs 既存
  - `fetch_page_transitions`: ページ間遷移 + URL簡略化
- 変更ファイル: `megaton_lib/slqm_analysis.py` (新規), `tests/test_slqm_analysis.py`
- テスト: 23件追加

**⑤ GSC認証の統合 → 解決**
- 課題: megaton と MCP で認証経路が異なり使い分け必要
- 解決: MCP側の `sa_corp-1872_gsc-api.json` を `credentials/` に symlink
- `build_registry()` が3つ目のサービスアカウントを自動発見
- `get_gsc('https://corp.shiseido.com/')` が megaton 経由で動作可能に

---

### 🔲 未着手の課題（優先度: 中〜低）

以下は長期的な改善項目として今後検討。

| # | 課題 | 優先度 | 内容 |
|---|------|-------|------|
| ④ | **megaton_lib のディレクトリ分割** | 中 | 汎用（core/）と案件固有（projects/talks, slqm, dei, with）の境界を明確化 |
| ⑥ | **megaton PyPIのステート管理改善** | 低 | `run()` が毎回新しい Result オブジェクトを返し、内部状態を変更しない設計に（破壊的変更） |
| ⑦ | **メトリクス/ディメンションのスコープ検証** | 低 | GA4 APIメタデータを使い、スコープ不一致を検知・警告 |

---

## A. 今回の分析で実際にぶつかった課題

### 1. アドホック分析がやりにくい → ✅ 解決済み（③で対応）

今回のような「70thの流入と回遊を多角的に調べる」作業では、`slqm.py` ノートブックの定型クエリは使えず、毎回 `mg.report.run()` を生で叩いた。megaton_lib にはSLQM用の高レベル分析関数がほぼない（`slqm_ga4.py` は `ym_from_year_month` や `compute_sp_ratio` など低レベルユーティリティのみ）。

→ ~~**ノートブックは「月次定型レポート」に最適化されていて、探索的分析の再利用性が低い**~~
→ `slqm_analysis.py` に8つのヘルパー関数を追加して解決。

### 2. BQコホート分析への到達が遠い → ✅ 解決済み（①で対応）

GA4 APIでは再訪問コホートが取れず、BQに切り替えた。しかし：
- `megaton_client.py` の `query_bq()` はパラメータ化クエリ非対応
- `talks_retention.py` の `init_bq_client()` は Talks 専用の設計（subsite, table_pv 等がハードコード）
- 結局 `init_bq_client` を流用して生SQLを書いた

→ ~~**BQの汎用的なパラメータ化クエリ実行が megaton_client に欠けている**~~
→ `query_bq()` にパラメータ化クエリ対応を追加、`get_bq_client()` を汎用化して解決。

### 3. GSCの認証経路が分断 → ✅ 解決済み（⑤で対応）

70thの検索クエリを調べようとして `get_gsc('https://corp.shiseido.com/')` → 認証エラー。megatonのサービスアカウントにはSLQMのGSC権限がなく、MCPサーバー（OAuth）でしか取れなかった。

→ ~~**同じデータソースに対して megaton と MCP で認証経路が異なり、使い分けが必要になる**~~
→ MCP側の `sa_corp-1872_gsc-api.json` を `credentials/` に symlink して統合。`build_registry()` が自動発見。

### 4. 滞在時間メトリクスの罠 → ✅ docstring で対応（⑦）

`userEngagementDuration` をイベントスコープで取ったら全部0。セッションスコープの `averageSessionDuration` に切り替えたら取れたが、どのメトリクスがどのスコープで動くか、megaton側にガードがない。

→ ~~**megatonがスコープ不一致を検知・警告する仕組みがない**~~
→ 自動検証の実装コストが高いため、`slqm_analysis.py` のモジュール docstring と `fetch_session_quality()` の docstring にスコープの罠を明記して対応。

### 5. `mg.report.data` の暗黙的な状態共有 → ✅ 緩和済み（②で対応）

`mg.report.run()` の結果は `mg.report.data` に格納される。複数クエリを連続実行すると前の結果が上書きされる。今回は毎回 `df_xxx = mg.report.data` で即座に退避したが、うっかり忘れるとデータが消える。

→ ~~**ステートフルな設計がアドホック分析で事故を起こしやすい**~~
→ megaton は既に `ReportResult` を返していた。`query_ga4()` と `slqm_analysis._run()` で `result.df` を直接使うように修正し、megaton_lib 内では `mg.report.data` を参照しない設計に統一。

---

## B. コード調査で見えた構造的な課題

### 6. megaton_lib の「汎用」と「案件固有」が混在

| 汎用（再利用可能） | 案件固有 |
|---|---|
| `date_utils`, `date_template`, `sheets`, `credentials`, `megaton_client` | `talks_*.py`, `slqm_ga4.py`, `slqm_analysis.py`, `dei_ga4.py`, `with_report.py` |

汎用モジュールと案件固有モジュールが同じ `megaton_lib/` に同居している。将来的にSLQMが終了した場合、案件固有コードの整理が煩雑になる。

### 7. BQクレデンシャルの二重経路 → ✅ 解決済み（①で対応）

~~- `megaton_client.py`: `start.Megaton(creds_path)` → Megaton内部でBQクライアント生成~~
~~- `talks_retention.py`: `GOOGLE_APPLICATION_CREDENTIALS` 環境変数 → 直接 `bigquery.Client()`~~

~~同じ認証情報を別の方法で初期化しており、どちらのパスが使われるかが暗黙的。~~

→ `megaton_client.get_bq_client()` に統合。`talks_retention.init_bq_client()` は deprecated ラッパーに変更。

### 8. megaton PyPIパッケージ自体の課題

- **ネストが深い**: `Megaton > Report > Run > ranges()` で4階層。内部で `parent.parent.parent` の参照チェーン
- **フィルタ構文が3種類**: GA4は `field==value`、GSCは `field=~regex`、BQはSQL。統一されていない
- **ReportResult/SearchResult が mutable**: `.df` を直接変更できてしまい、ラッパーの状態と乖離する

---

## C. 改善案（優先度順）

### 高：今回の分析を楽にするもの

#### ✅ ① `megaton_client.py` にパラメータ化BQクエリを追加（完了 2026-02-16）

`query_bq()` を拡張し、`params` 引数でパラメータ化クエリに対応。`get_bq_client()` を新設して `google.cloud.bigquery.Client` を直接取得可能に。`talks_retention.init_bq_client()` は deprecated ラッパーに変更。

変更ファイル:
- `megaton_lib/megaton_client.py` — `get_bq_client()` 新設、`query_bq()` 拡張
- `megaton_lib/talks_retention.py` — `init_bq_client()` を deprecated 委譲に変更
- `tests/test_megaton_execution.py` — テスト6件追加

```python
# 使用例
df = query_bq("project-id", "SELECT * FROM t WHERE month = @m",
              params={"m": "202602"}, location="asia-northeast1")
```

**付随改善（①-b: 認証解決の構造化）:**
BQ認証のインライン処理を4関数に分解し、解決順序を明文化:
- `_select_credential_path()` — 候補JSONからhint一致優先で選択
- `resolve_bq_creds_path()` — GAC環境変数 → 候補探索の解決順序
- `ensure_bq_credentials()` — 必要時に `GOOGLE_APPLICATION_CREDENTIALS` をセット
- `describe_auth_context()` — デバッグ用の認証状態可視化
- `docs/REFERENCE.md` — 認証解決ルールのドキュメント追加
- `docs/USAGE.md` — BQ認証の補足追加

#### ✅ ② `query_ga4()` が `ReportResult.df` を直接返すように修正（完了 2026-02-16）

megaton の `mg.report.run()` は既に `ReportResult` オブジェクトを返していた。megaton PyPI 側の変更は不要。`megaton_client.query_ga4()` と `slqm_analysis._run()` で `result.df` を使うように修正し、ステートフルな `mg.report.data` への依存を排除。

変更ファイル:
- `megaton_lib/megaton_client.py` — `query_ga4()`: `result.df` を返すように修正
- `megaton_lib/slqm_analysis.py` — `_run()`: 同上
- `tests/test_megaton_execution.py`, `tests/test_megaton_registry.py` — モック修正

#### ✅ ③ SLQM用の分析ヘルパー関数群を追加（完了 2026-02-16）

`slqm_analysis.py` を新設し、70th 分析で繰り返し使ったクエリパターンを8関数に整理:

| 関数 | 用途 | 戻り値の主な列 |
|---|---|---|
| `fetch_daily_metrics` | 日別 UU/sessions/PV | date, uu, sessions, pv |
| `fetch_page_metrics` | ページ別指標 + 読了率 | page, uu, pv, footer_views, read_rate |
| `fetch_channel_breakdown` | チャネル別流入 | channel, uu, sessions |
| `fetch_source_medium` | ソース/メディア別流入 | source_medium, uu, sessions |
| `fetch_landing_pages` | ランディングページ別 | landing, uu, sessions |
| `fetch_session_quality` | 滞在時間・ページ/セッション | landing, uu, sessions, avg_duration, pages_per_session |
| `fetch_new_vs_returning` | 新規 vs 既存 | user_type, uu, sessions |
| `fetch_page_transitions` | ページ間遷移 + URL簡略化 | from_page, to_page, users, from_short |

変更ファイル:
- `megaton_lib/slqm_analysis.py` — 新規作成
- `tests/test_slqm_analysis.py` — テスト20件
- `megaton_lib/__init__.py` — ドキュメント更新

**付随修正（Codexレビュー対応）:**
- `dei_ga4.py`: `classify_source_channel()` の `ai_name.capitalize()` バグ修正（"ChatGPT" → "Chatgpt" 問題）
- `megaton_client.py`: `query_bq()` の `location` デフォルトを `None` に変更（固定値 `"asia-northeast1"` の排除）
- `slqm_analysis.py`: `fetch_source_medium()` / `fetch_landing_pages()` の `limit` 引数が `_run()` → `mg.report.run()` に伝播されていなかったバグを修正。テスト追加。

#### ✅ ③-b 共通GA4ヘルパーの抽出（完了 2026-02-16）

`slqm_analysis.py` 内の `_run()` / `_date_col()` およびインライン `pd.to_numeric` パターンを汎用モジュール `ga4_helpers.py` に抽出:

| 関数 | 用途 |
|---|---|
| `run_report_df()` | `mg.report.run()` → DataFrame 変換（limit 対応含む） |
| `build_filter()` | フィルタ文字列を `;` 結合（空/None スキップ） |
| `to_datetime_col()` | date 列を datetime に変換 |
| `to_numeric_cols()` | 指定列を数値化（fillna/astype 対応） |

変更ファイル:
- `megaton_lib/ga4_helpers.py` — 新規作成
- `tests/test_ga4_helpers.py` — テスト10件
- `megaton_lib/slqm_analysis.py` — `ga4_helpers` の関数に置き換え、スコープの罠を docstring に追記

#### ✅ ③-c ノートブック側の `mg.report.data` 依存排除（完了 2026-02-16）

`megaton-notebooks/notebooks/reports/slqm.py` の `get_metric_df()` を `result.df` パターンに修正。`prep_rules` 使用時は `mg.report.prep(conf, df=result.df)` で `mg.report.data` への依存を排除。

### 中：構造を整理するもの

#### ✅ ⑧ 公開API境界の入力バリデーション（完了 2026-02-16）

`query_ga4()` / `query_gsc()` / `query_bq()` に渡す引数を、内部に入る前に検証・正規化するレイヤーを挿入。

型定義:
- `FieldSpec` — `str | tuple[str, str]` のエイリアス
- `GscDimensionFilter` — TypedDict で `dimension`, `operator`, `expression` を必須に
- `AuthContext` — `describe_auth_context()` の戻り値型

バリデーション関数（全て内部 `_` prefix）:
- `_normalize_fields()` — str/tuple 以外・空文字列を reject、strip 済みで返す
- `_normalize_gsc_dimension_filter()` — 必須キーの存在確認
- `_normalize_bq_params()` — 値を `str()` で文字列化、`None` は `None` を保持（SQL NULL対応）

変更ファイル:
- `megaton_lib/megaton_client.py` — バリデーション関数追加、公開API型ヒント強化
- `tests/test_megaton_execution.py` — テスト5件追加

#### ✅ ⑨ バッチ実行のエラーレポート構造化（完了 2026-02-16）

自由形式だったエラー情報を `error_code` / `message` / `hint` の統一スキーマに整理。

- `batch_runner.py` — JSON パースエラーに `path`/`hint` 追加、`run_batch` ループで構造化フォールバック補完、例外に `details.exception_type` 追加
- `scripts/query.py` — `_execute_single_config` の3段階（クエリ実行→パイプライン→保存）に個別 try/except と段階別 `error_code`（`QUERY_EXECUTION_FAILED`, `NO_DATA_RETURNED`, `PIPELINE_FAILED`, `SAVE_FAILED` 等）

#### ✅ ⑩ テスト基盤の整備（完了 2026-02-16）

`pyproject.toml` に pytest マーカー定義を追加（`unit`, `contract`, `integration`）。`tests/conftest.py` にレイヤードマーカーの自動適用を設定。

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

#### ✅ ⑤ GSC認証の統合（完了 2026-02-16）

MCP側のみが持っていた `sa_corp-1872_gsc-api.json`（corp.shiseido.com のGSC権限付き）を `credentials/` に symlink で統合。

```bash
ln -s ~/Dropbox/python/key/sa_corp-1872_gsc-api.json credentials/sa_corp-1872_gsc-api.json
```

これにより `build_registry()` が3つ目のサービスアカウントを自動発見し、`get_gsc('https://corp.shiseido.com/')` が megaton 経由で動作可能に。MCP側は引き続き Dropbox の実ファイルを参照するため、同一ファイルの二重参照で整合性を維持。

### 低：長期的な品質向上

#### ⑥ megaton PyPIのステート管理改善

`mg.report.data` の暗黙上書きを防ぐため、`run()` が毎回新しい Result オブジェクトを返し、内部状態を変更しない設計に。（破壊的変更になるので慎重に）

> **Note**: ②で megaton_lib 側は `result.df` を直接使う設計に統一済み。megaton PyPI 側の変更は未着手だが、megaton_lib のユーザーコードでは問題が発生しにくくなっている。

#### ⑦ メトリクス/ディメンションのスコープ検証 → docstring で対応（完了 2026-02-16）

~~GA4 APIのメタデータを使い、イベントスコープのクエリにセッションスコープのメトリクスを混ぜた場合に警告を出す。~~

自動検証の実装コスト（メタデータAPI呼び出し + キャッシュ + 検証ロジック）に対してリターンが小さいため、docstring での注意喚起で対応。`slqm_analysis.py` のモジュール docstring に `userEngagementDuration` の罠と一般的なスコープ注意事項を記載。

---

## D. 進捗サマリー

| # | 施策 | 状態 | 対応日 |
|---|---|---|---|
| ① | パラメータ化BQクエリ + 認証解決構造化 | ✅ 完了 | 2026-02-16 |
| ② | ReportResult.df 直接返却 | ✅ 完了 | 2026-02-16 |
| ③ | SLQM分析ヘルパー + ga4_helpers 抽出 + NB修正 | ✅ 完了 | 2026-02-16 |
| ④ | ディレクトリ分割 | 未着手 | — |
| ⑤ | GSC認証統合 | ✅ 完了 | 2026-02-16 |
| ⑥ | ステート管理改善 | 一部緩和 | — |
| ⑦ | スコープ検証 | docstring で対応 | 2026-02-16 |
| ⑧ | 公開API入力バリデーション | ✅ 完了 | 2026-02-16 |
| ⑨ | バッチエラーレポート構造化 | ✅ 完了 | 2026-02-16 |
| ⑩ | テスト基盤整備（pytest マーカー） | ✅ 完了 | 2026-02-16 |

全479テスト pass。

---

## 背景: 今回の分析で行ったこと

SLQM セクション (`corp.shiseido.com/slqm/`) の 2/4 新コンテンツ (`/70th/`) 公開後の分析:

1. **日別 UU/PV バーチャート** — 2/4 に通常の5倍のスパイク確認
2. **ページ別指標 + 読了率** — history.html 37.2%が最高
3. **流入チャネル比較** — Organic Social (Facebook中心) が12倍増
4. **ソース/メディア別** — facebook/social 434 sessions が最大
5. **回遊分析** — 70th内の回遊構造 + 70th→トップ→商品の2段階回遊
6. **滞在時間** — 70th着地は29.5min vs 既存21.7min
7. **BQコホート定着分析** — 70th着地新規 2.8% vs 既存ページ着地 4.8%
8. **ソース別ファネル** — 70th着地に限定。読売QR (再訪問7.9%, ショップ到達26.3%) が突出
9. **GSC検索クエリ** — 70th配下はクリック4件のみ、SEO未成熟

使用ツール: megaton (PyPI) + megaton_lib + BQ直接クエリ + GSC MCP サーバー
