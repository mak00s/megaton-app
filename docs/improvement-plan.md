# ライブラリ改善計画 (megaton / megaton-app / megaton-notebooks)

> **これは進行管理の正本。** AIセッションが切れたら、まずこのファイルを読んで「現在地」から再開すること。
> 各ステップ完了時にこのファイルのチェックボックスと「現在地」を更新してコミットする。

最終更新: 2026-06-11 / 状態: **Step 1〜7 完了(8は基準のみ)。残タスクは下記**

## 残タスク(進行中)

1. **デプロイ(承認済み: push + PyPI publish、順序厳守)**: ① megaton push + PyPI v1.4.0 → ② megaton-app push → ③ notebooks ピン更新 → ④ notebooks push。slqm検証パス後に実行
2. **slqm.py 実走検証(進行中 2026-06-11)**: 共有ドライブ「WITH Report」に一時コピー `_tmp_slqm_verify_20260611`(id 1K8drkU4cDN3SeTdJpe-izD1ecMt6E3FltmiVxKWQsAM)を作成し実走。
   - 1回目: `_page-d` / `_page-m` / `_info` は本番と**全セル一致**。`_page` / `_all-m` は列欠落 → 原因は移行ではなく **GA4 504 Deadline Exceeded → リトライ枯渇 → megatonが黙って空を返す**仕様(列が静かに消え、validationはpassed表示)。重いlinkUrlフィルタ系5クエリで発生
   - **megaton 2581e2f で修正**: リトライ枯渇はデフォルト例外送出(`on_exhausted='empty'`で旧挙動)、リトライ3→5回。docs/CHANGELOG更新済み
   - 2回目を強化版で実行中 → 全タブ一致なら検証完了。**終了後に一時コピーを削除すること**
3. [x] shibuya-line WIP合流(78a1f15)後の整理完了(0ef562a): bootstrap除去・pd_utils削除・report-catalog更新
4. report_run の横展開(検証パス後: shibuya.py → 残り)。チェーンAPI(`wrap`/`.month_key()`)置換も同時に
5. GHA `-e .` 初回実行モニタ / `output/tmp_verify_slqm_compare.py` は検証完了後に削除

## 完了コミット一覧

- megaton: dcbb3b9(公開API) → 89f687a(wrap/chain) → 2ec7364(traffic/_ResultBase) = **v1.4.0、508 tests**
- megaton-app: 8a1dd19 → 253c0dd → 40bd9e4 → ccb49b1 → bc2da62 → 060bf12 → ea028a7 = **v0.15.0+、941 tests**
- megaton-notebooks: 09ba421 → 53537c3 → 265a329 → 530eb9b = **96 tests**。ユーザーWIP(shibuya-line系+report-catalog.md)は未コミットのまま温存

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
- [x] **Step 2: チェーンAPI正準化 + transform一本化** ✅ 完了 (2026-06-11, megaton 89f687a+2ec7364, app ccb49b1)
  - [x] `megaton.wrap(df, dimensions=None)` 公開(非数値列をdim推定、package rootから遅延export)
  - [x] `mg.save/append/upsert.to.*` と `mg.sheet.save/append/upsert` がResult受け入れ(`_extract_df`)
  - [x] `.month_key()` 追加、`transform.fillna_int` 追加(pd_utils昇格)。tests/test_wrap_and_chain.py 15件
  - [x] traffic汎用プリミティブ(normalize_domain/source_host/is_non_public_dev_source/ensure_trailing_slash/apply_source_normalization)を `megaton.transform.traffic` へ昇格、megaton_lib.traffic は再エクスポート(shibuya-analysis/dei.py のimportは無変更で動く)。row-level classify_channel/reclassify_source_channel はビジネス既定値を含むため megaton_lib に残置
  - [x] `_ResultBase` 導入: normalize/categorize/_map_value/_normalize_value の重複を統合(`_with_df` フック)。挙動不変、megaton 508テストパス
  - 判断メモ: table_utils.apply_pattern_map は transform.map_by_regex と**意図的に挙動が違う**(大小文字区別・str化・警告)ため委譲しない。docstringで新規コードは transform/chain API を使うよう誘導。megaton.transform.ga4.classify_source_channel(df-level, custom_channels対応)が分類の正準で、traffic row-level版は旧世代として共存
- [x] **Step 3: Sheets retry充当** ✅ 完了 (2026-06-11, app 253c0dd)
  - [x] gspread_lowlevel に公開 `call_with_retry(op, func)` 追加(expo_retry + 429に30秒フロア)、全ネットワーク呼び出し(open/worksheet/clear/update/freeze/append/get_all_values/batch_update/metadata)をラップ
  - [x] report_validation._with_retry を expo_retry ベース+30秒フロアへ(旧実装は最大7秒でquota窓に不足していた)
  - [x] tests/test_gspread_lowlevel_retry.py (5件)
- [x] **Step 4: 日付ファサード megaton_lib.dates** ✅ 完了 (2026-06-11)
  - [x] date_template に prev-prev-month-start/end + resolve_month() 追加
  - [x] megaton_lib/dates.py 新設: 文字列API(resolve_date/resolve_month)+ dateオブジェクトAPI(date_periods由来: today_in_timezone/previous_month_window/month_before_window/resolve_period_date/resolve_period_month/previous_month_label)+ pandas月ヘルパー + parse_summary_tokens + resolve_effective_months_ago(pandas依存を除去して昇格)
  - [x] notebooks: date_periods削除、消費3ファイル(slqm_looker_export/build_with_summary/run_corp_bq_derivatives)をmegaton_lib.datesへ置換、notebook_paths.resolve_effective_months_agoは委譲化(レポートのimportは無変更)、lib-modules.md更新
  - [x] tests: test_dates_facade.py 19件(UTC境界テスト移植込み)、notebooks 102件パス
- [x] **Step 5: report_run scaffold + bootstrap根絶** ✅ ほぼ完了 (2026-06-11, app bc2da62, notebooks 53537c3)
  - [x] megaton_lib/report_run.py: `start_report_run()` — **notebookはセル構造でwithブロックを跨げないため begin/end ペアが主形**(`run = start_report_run(...)` → `run.finish()`)。scriptはcontext manager可。on_finishフック、save_sheet等のmg自動渡し。tests 16件
  - [x] gsc_utils.fetch_for_sites()(マルチサイトGSCループの共通化)
  - [x] notebooks editable install化(`-e .`)→ レポート9本のbootstrap削除。dei/with-kpi の PROJECT_ROOT は新設 `lib.notebook_paths.repo_root()` で代替
  - [x] パイロット slqm.py を report_run 移行(444→426行)。slqm.md仕様書も更新(※reports/*.md は自動生成ではなく手書き仕様書なので注意)
  - [x] AGENTS.md にパラメータ命名+初期化規約を追記
  - [ ] **残**: shibuya-line.py のbootstrap(ユーザーWIP中のため未除去、次回改修時に)/ ops scripts のbootstrap(Step 7で)/ **slqm.py の実走検証(§9 sheet equality)は未実施** — 本番Sheetsへ書くため勝手に実行していない。次回の手動実行か7/1定期実行前に確認すること
  - [ ] 残: shibuya.py 等への report_run 横展開(slqm検証後)
- [x] **Step 6: facade + Python APIドキュメント** ✅ 完了 (2026-06-11, app 060bf12)
  - [x] megaton_lib/notebook.py = PEP 562 遅延facade(16個: get_ga4/get_gsc/get_bq_client/query_*/wrap/resolve_date/resolve_month/read_sheet_table/save_sheet_table/upsert_or_skip/start_report_run/fetch_for_sites/fillna_int/show)。旧init()も残置
  - [x] docs/PYTHON_API.md 新設(正準形: チェーンAPI/dates/report_run/してはいけないこと)。README/AGENTS.mdからリンク
- [x] **Step 7: 昇格 + wrapper削除** ✅ ほぼ完了 (2026-06-11, app ea028a7, notebooks 265a329+530eb9b)
  - [x] env_utils / google_workspace → megaton_lib へ移動(credentialsとの統合はせず独立モジュールのまま — 発見と構築で役割が違うため)
  - [x] sheets_utils + sheets_requests → gspread_lowlevel に統合(column_label/gs_serial_to_date/dimension_requests/copy_format_request)、テストも移管
  - [x] notebooks lib/report_validation.py 削除(消費ゼロ)。pd_utils は shim 化(shibuya-line WIPが使用中のため、WIP合流後に削除)
  - [x] ops scripts 6+1本の bootstrap も削除(corp-ppt の `_root` 参照は repo_root() へ)
  - 判断: box.py は純再エクスポートではなく実ロジック(file-URL除外フィルタ)を持つため残置。notebook_paths は repo_root()新設・日付委譲済みで十分スリム(detect_workspace_rootsはまだ5ファイルが使用)
- [ ] **Step 8: product固有抽出** — 基準のみ: 2プロダクト以上で同型確認時に昇格。先回りしない(現状対象なし)

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
