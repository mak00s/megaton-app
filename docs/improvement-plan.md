# ライブラリ改善計画 (megaton / megaton-app / megaton-notebooks)

> **これは進行管理の正本。** AIセッションが切れたら、まずこのファイルを読んで「現在地」から再開すること。
> 各ステップ完了時にこのファイルのチェックボックスと「現在地」を更新してコミットする。

最終更新: 2026-06-11 / 状態: Step 1 完了(megaton dcbb3b9 + app 8a1dd19)、Step 3 着手

## ゴール

notebook / py での処理・分析・レポート生成を「短く・シンプルに・直感的に」。
チェーンAPI(ReportResult/SearchResult)を正とし、純関数(megaton.transform)の上に被せる二層構造を完成させる。

## 合意済みの方針

- **チェーンAPI正準化は合意済み**。条件: (a) `megaton.wrap(df)` で任意DataFrameをチェーンに乗せる、(b) `mg.save.to.*` がResultを直接受ける、(c) 重複は megaton.transform へ寄せる。
- **後方互換は最低限でよい**(megaton-appの利用者は本人のみ)。ローカルの全消費repoを移行すれば旧名は削除可。
- megaton (PyPI) のみ追加的変更とし、破壊的変更はしない。

## 消費repo実態調査 (2026-06-11 実施済)

今回触るモジュールの消費者:

| モジュール | 消費repo | 対応 |
|---|---|---|
| megaton_client (get_ga4/get_gsc/get_bq_client/query_*) | megaton-notebooks, shibuya-analysis, shiseido-analysis, dms(query_aa) | 公開関数の署名不変。内部のみ改修 |
| traffic / table_utils | shibuya-analysis, megaton-notebooks | megaton.transformへ移し、megaton_lib側にre-exportエイリアス維持 |
| sheets / bigquery_utils | shibuya, shiseido, wws, notebooks | 署名不変(retry追加のみ) |
| date_template / date_utils / periods | megaton-notebooks のみ | ファサード化、実装モジュールは残す |
| megaton.gsheet (MegatonGS直接) | hs_ai_workflow | megatonは追加のみ、無影響 |
| playwright_browser | expense, notebooks, shiseido | 触らない |
| audit/validation サブパッケージ | csk, dms, scanbe, wws, shiseido | **今回のスコープ外。触らない** |

adobe-md, minkabu は import なし。

## ステップ一覧と現在地

- [x] **Step 1: megaton v1.4 プログラム用公開API** ✅ 完了 (2026-06-11)
  - [x] `Megaton.for_property()` / `Megaton.for_site()` — headlessデフォルトtrue(自動判定は不採用: ファクトリ=プログラム用と整理)
  - [x] `mg.properties()` / `mg.sites()` / `mg.use_property(id, refresh_metadata=)` — refresh_metadata=False は旧 `property.id=` 直代入ハックの正式版(多プロパティloop用)
  - [x] 複合フィルタ dict形式 `{"and"/"or"/"not": ...}` (filter_d/filter_m)。order_byは既存 `sort=` が対応済みと判明、追加不要
  - [x] tests/test_programmatic_api.py (18件)・CHANGELOG 1.4.0・api-reference.md
  - [x] 下流置換: megaton_client.py(build_registry/get_ga4/get_ga4_properties/get_gsc_sites/query_gsc)、ga4_helpers.py×2、scripts/fetch_*_megaton.py×2。テストfake更新済
  - 注: megatonはeditable installなのでapp側は即1.4を参照。PyPIへのpublishは未実施(全ステップ完了後にまとめて)
- [ ] **Step 2: チェーンAPI正準化 + transform一本化** (megaton, megaton-app)
  - [ ] `_ResultBase` 共通基底、transform純関数への委譲完成
  - [ ] `megaton.wrap(df)` 公開
  - [ ] `mg.save.to.*` がResult受け入れ
  - [ ] `.month_key()` 追加、transform.table に `fillna_int` 追加
  - [ ] megaton_lib table_utils/traffic → transform委譲エイリアス化(shibuya-analysisのimportは壊さない)
- [ ] **Step 3: Sheets retry充当** (megaton-app) ※独立、いつでも可
  - [ ] gspread_lowlevel に expo_retry ラップ(`_retrying()` デコレータ)
  - [ ] report_validation._with_retry を expo_retry ベースへ
- [ ] **Step 4: 日付ファサード megaton_lib.dates** (megaton-app, notebooks) ※独立
  - [ ] date_template に prev-prev-month-start/end + resolve_month() 追加
  - [ ] megaton_lib/dates.py 新設(全日付APIの単一入口、resolve_effective_months_ago昇格込み)
  - [ ] notebooks: lib/date_periods 削除、import置換
- [ ] **Step 5: report_run scaffold + bootstrap根絶** (megaton-app, notebooks) ※Step 4後
  - [ ] notebooks editable install化(pyproject packages=["lib"], requirements に -e .)→ bootstrap 8本×10行削除
  - [ ] megaton_lib/report_run.py (context manager: creds→期間→tracker→summary、on_finishフック)
  - [ ] gsc_utils.fetch_for_sites() 追加
  - [ ] パイロット: slqm.py → 検証 → shibuya.py → 残り
  - [ ] パラメータ命名規約を notebooks AGENTS.md へ(既存の一括リネームはしない)
- [ ] **Step 6: facade + Python APIドキュメント** (megaton-app) ※Step 2後
  - [ ] megaton_lib/notebook.py を PEP 562 遅延facadeに(12個: get_ga4, get_gsc, query_gsc, get_bq_client, query_bq, wrap, resolve_date, resolve_month, read_sheet_table, save_sheet_table, upsert, report_run, show)
  - [ ] docs/PYTHON_API.md 新設
- [ ] **Step 7: 昇格 + wrapper削除** (megaton-app, notebooks) ※Step 4/5後
  - [ ] env_utils / google_workspace(credentialsと統合)/ sheets_utils+sheets_requests(gspread_lowlevelへ)昇格
  - [ ] notebooks lib/report_validation.py(純再エクスポート)削除、box.py簡素化、notebook_paths解体
- [ ] **Step 8: product固有抽出** — 2プロダクト以上で同型確認時のみ。先回りしない

## リリース・移行の運用

- 順序: megaton → megaton-app → 消費repo(notebooks等)のピン更新
- megaton-notebooks は requirements.txt で megaton-app をコミット固定ピン → ステップ完了ごとに更新
- 旧名エイリアスは「全消費repoでgrep使用ゼロ確認」後に削除してよい(利用者は本人のみ)
- レポート改修は AGENTS.md §9 のシート出力一致プロトコルで検証

## 設計メモ(セッション間で忘れやすいもの)

- チェーンメソッドは全て新Resultを返す不変設計(start.py)。transform委譲は clean_url 等で一部実装済み。
- megaton.transform には map_by_regex / clean_url / group_sum / weighted_avg / classify_by_regex / classify_channel / fillna対象の table 系が既存。megaton_lib.traffic.classify_channel と megaton.transform.ga4.classify_channel は重複(Step 2で突合)。
- get_bq_client は megaton_client に既存(過去調査の「無い」は誤り)。
- notebooks の groupby→fillna_int→sort 三点セット(8回以上反復)は `.group().to_int().sort()` で表現可。
- 月キーformat drift: "%Y/%m/1" / "%Y%m" / "%Y-%m-01" → `.month_key()` で統一予定。
- megaton_client の内部アクセス箇所: headless強制 L216 / accounts L232-234,358-370 / account・property.select L320-325 / search.get.sites L239,444 / search.data L486。
