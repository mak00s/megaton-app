# 利用repoの共通化棚卸し

調査日: 2026-09-12。状態: 優先3項目のローカル実装・テスト完了。共通側の提供版はv0.32.0。利用repoの固定版実行環境への反映は未実施。

## 実装結果 (2026-09-12)

- **Sheets**: Minkabuのbatch送信を `batch_update_spreadsheet` へ移行。ShibuyaはJWT手書き署名・urllib通信・独自retryを削除し、google-auth/gspread/megatonへ委譲した。既存関数の返却形式と値の指定モードを維持。JIDVのグローバル通信関数差し替えは読取の `timeout=120` 指定へ置換。Shibuyaのgspread依存下限と日次実行pinは検証済みの6.2.1を明記。
- **Tags**: CSKのDE設定・解除・削除、TEST_PNのrule revision更新を公開APIへ移行。共通のlibrary summaryに `origin_id` を追加し、解除にはlibrary revision ID、削除にはorigin IDを渡す。CSKの進行中のWebSDK/STG処理は保持した。DE作成等に残るprivate APIは今回の移行対象外。
- **CSV**: 比較本体を `megaton_lib.csv_validation` へ抽出。notebooksの既存関数はWITHデフォルトを補う薄いwrapperとなり、Shiseidoの `notebook_helpers()` 経路からも同じ実装を使用する。Shiseidoの呼び出しコード変更は不要で、実際のhelper経由で正常系・不一致系をオフライン確認した。

検証結果:

| 対象 | 結果 |
| --- | --- |
| Minkabu `tests/test_sheets.py` | 28 passed (system Pythonとrepoの`.venv`の両方) |
| Shibuya `tests/` 全体 | 226 passed (transport/既存report goldenを含む) |
| megaton-app Tags provider | 35 passed |
| megaton-app CSV validation | 12 passed |
| CSK 対象3テストファイル | 21 passed |
| notebooks CSV/file-versioning/GA-followup/WITH-completion | 21 passed |
| Shiseido WITH ops | 21 passed |

Minkabu以外の上記テストは `/Users/maks/.pyenv/versions/3.12.11/bin/python` を使用し、megaton/megaton-appのローカルeditableを参照した。
Shibuyaの既存`.venv`はmegaton_lib未導入、notebooksの既存`.venv`はmegaton未導入だった。既存Makefileの通常Python経路で検証し、これらのvenvは変更していない。
本番API実行・本番Sheets全セル比較・Tags build/publishは行っていない。

適用順序: megaton-appの新CSVモジュールとorigin ID追加を先にリリースし、notebooksの `requirements.txt` の共通側pinを更新してからnotebooks変更をActionsへ反映する。
CSKも新しい共通側を必要とする。Sheets側のbatch APIはmegaton v2.1.1に既に存在し、megaton側の追加実装は不要。
利用repoの依存pin更新・commit/pushは共通側v0.32.0の公開後に行う。
WITH固有の期待データ構築・前月比較・GCS処理が残るため、notebooks checkoutへの依存全体は未解消。

以下は実装前の棚卸し記録。

## 対象と判断基準

2026-07-12〜2026-09-12 のローカルGit履歴から変更箇所を抽出し、主要候補の現在の実装と共通APIを照合した。
全コードのバグレビューや本番実行頻度の測定ではない。remoteの最新状態・実環境のインストール版は未確認。
責務の正本は [architecture.md](architecture.md)。本書はこの時点の調査記録であり、API仕様ではない。

| Repo | 調査時HEAD | 期間内コミット数 | 変更されたPythonファイル数 |
| --- | --- | ---: | ---: |
| csk-analysis | `83ead49` | 78 | 27 |
| wws-analysis | `64c0a3c` | 3 | 3 |
| dms-analysis | `a4894fb` | 7 | 18 |
| megaton-notebooks | `089202a` | 16 | 17 |
| shibuya-analysis | `f7a0c99` | 45 | 44 |
| shiseido-analysis | `85bd847` | 142 | 53 |
| minkabu | `7e1e47f` | 56 | 28 |

ファイル数は期間内コミットの変更パスを重複排除し、`tests/` と `test_*.py` を除いた数。削除済みパスや `__init__.py` も含む。
notebooks は `/Users/maks/Repos/megaton-notebooks` を使用し、同じremoteを持つ別checkoutは合算していない。
CSK・WWS・Shiseidoには未コミット変更がある。以下のソース参照は調査時worktreeであり、特にCSKの候補は移行前に再確認する。
共通側HEADは `3beaaaa`。開始時から `README.md`、`query_normalization.py` とそのテストに別作業の変更があり、本調査では変更していない。

## 優先順位

### 1. Sheets通信・リトライを既存APIへ移行

分類: **既存APIへの移行**。所有者: `megaton`。優先度: 高。

- Shibuya: [offline_integration_gs.py](../../../shibuya-analysis/analysis/offline_integration/lib/offline_integration_gs.py) は `build_access_token()` でJWTを組み立て、opensslで署名する。`urlopen_with_retry()`、値のread/write、`batch_update()` も独自実装。
- 同ファイルの `batch_update()` は操作種別を見ずリトライし、`add_sheet_request`、`delete_sheet_request`、行列拡張の呼び出しにも使われる。
- Minkabu: [sheets.py](../../../minkabu/src/minkabu/sheets.py) の `_batch_update()` は `_sheets_retry` で一律リトライされ、`append_rows()` からも呼ばれる。共通retry関数を呼ぶだけでは、操作ごとの再送可否まで統一できていない。
- 共通側には [gsheet_lowlevel.py](../../megaton/megaton/gsheet_lowlevel.py) の `batch_update_spreadsheet(retry=None)` があり、batch全体の操作を判別する。構造変更の再送を避ける既存改善を利用側へ届ける価値が大きい。

進め方: Minkabuのbatch送信部分を先に置換し、続いてShibuyaの認証・I/Oを移行する。外側の無条件retryも除かないと意味がない。SA情報の選択順序は呼び出し側に残し、認証処理はgoogle-authを使用する。現在の `google_workspace.build_service_account_credentials()` はファイル入力なので、環境変数内JSONを一時ファイルへ変換するだけの新wrapperは作らない。

維持する契約: RAW/USER_ENTERED、FORMULAでの読取、空セル・日付・数値の扱い、対象範囲、案件のタブ名・表示規則。
検証: 429/503時のread再試行、append/addを含むbatchの単発送信、既存リクエストpayloadの等価性。Shibuyaのreport goldenテストも使用する。本番Sheetsへの書込はこの調査では行っていない。

### 2. CSKのTags private呼び出しを公開APIへ置換

分類: **既存APIへの移行を先行、不足分のみ共通側へ戻す**。優先度: 高。

- [provision_websdk_webinar_entity_send_rule.py](../../../csk-analysis/tags/provision_websdk_webinar_entity_send_rule.py) はDE設定変更・library解除・削除を `_reactor_patch` / `_reactor_delete` で組み立てる。
- [connect_shared_readiness_test_pn.py](../../../csk-analysis/tags/connect_shared_readiness_test_pn.py) は `_revise_library_resources` を直接importしている。
- 既存の [adobe_tags.py](../megaton_lib/audit/providers/tag_config/adobe_tags.py) には `apply_data_element_settings`、`remove_library_resources`、`delete_resource`、`revise_library_rules` がある。

進め方: 上記公開APIへ移行する。`revise_library_rules` は返却キーが `new_rule_ids` なので単純な名前置換では済まない。解除APIが要求するのはrevision IDであり、origin IDからの解決は別途確認する。設定・削除APIのdry-runではreadが発生し得るため、従来の通信有無も確認する。
DE作成やorigin/revision解決がなお複数スクリプトに残る場合に限り、既存providerへ狭い公開操作を追加する。private関数の一括公開はしない。

検証: submitted/dirty資産、originとrevisionの違い、dry-run時の無変更、再実行時に余分な作成がないこと。CSKの対象資産名、readiness条件、WebSDKの送信内容はCSKに残す。

### 3. WITH CSV検証のcheckout間依存を解消

分類: **共通側へ戻す候補**。所有者: `megaton_lib` の分析入力・検証処理。優先度: 中。

- Shiseidoの [ops/with_csv.py](../../../shiseido-analysis/ops/with_csv.py) の `notebook_helpers()` はnotebooksのパスを `sys.path` に追加し、`lib.file_versioning` / `lib.ga_import_validation` などを直接importする。
- notebooksの [notebooks/ops/with-csv.py](../../../megaton-notebooks/notebooks/ops/with-csv.py) も同じ検証を利用する。コードのコピーではないが、インストール済み共通ライブラリ以外のcheckout配置に依存している。
- [file_versioning.py](../../../megaton-notebooks/lib/file_versioning.py) にファイル集合・CSVヘッダー・前回との差の検証、[ga_import_validation.py](../../../megaton-notebooks/lib/ga_import_validation.py) に `validate_generated_import_csv()` がある。

進め方: まず期待DataFrameと生成CSVのヘッダー・キー・値の比較だけを抽出し、両方の呼び出し元を移行する。年齢区分、社員データ構築、`with_id`、「前回と行数が同じなら失敗」、GA反映の合格条件はWITHの方針として残す。
`notebook_helpers()` 全体やGCS削除・Gmail操作まで一括で移さない。共通化完了後も残るcheckout依存を明記する。

検証: BOM/文字コード、空キー、重複キー、列順、行順、欠損値、同数だが内容が異なるCSV。既存fixtureを両方の入口で使い、エラー条件を意図せず変更しない。

### 4. DMSのclassification実行状態は抽出前に契約を整理

分類: **抽出保留・設計候補**。優先度: 中〜低。

[classifications/routine.py](../../../dms-analysis/classifications/routine.py) の `apply_plan()` は入力hash・コードhash・live AA状態を再確認し、失敗時に `apply_failed_uncertain` として再送を止める。API呼び出し自体は既存 `ClassificationsClient` に委譲済み。

共通化候補はimmutableなplanと実行状態・証跡の管理。ただし今回確認した他repoに同じ契約の実装はなく、すぐ汎用workflow engineにはしない。別のAA import運用が必要になった時に抽出する。prop別のレビュー表示、suite確認、分類規則、承認条件はDMSに残す。

### 5. GTM編集・返信draftは不足があるが、対象を限定

分類: **共通側の機能候補、現時点では保留**。優先度: 低。

- Shiseidoの [sync_talks_gtm.py](../../../shiseido-analysis/analysis/sync_talks_gtm.py) はGTM探索、fingerprint付きvariable更新、quick previewを実装する。共通 [gtm.py](../megaton_lib/audit/providers/tag_config/gtm.py) は取得・同期中心。第2の編集利用先ができた時に、ページ別JavaScript生成からAPI操作だけ分離する。workspace名・変数名・画像検査は利用側に残す。
- WITHの `create_reply_draft()` はthread IDと返信ヘッダーを指定するが、共通 [gmail_client.py](../megaton_lib/gmail_client.py) の `create_draft()` にはその引数がない。現APIへそのまま置換すると返信の紐付けを失う。分析成果・反映確認の返信という用途に限定して将来拡張を検討し、宛先選定・本文・送信承認は呼び出し側に残す。

### 6. 今回は共通側へ移さない処理

- Shibuyaの予約帰属・院別ルール・AI流入の分類・レポート表示規約。
- CSKの推薦枠・疾患/製品分離・readinessイベントの具体的な条件。
- DMSの行動persona・N1選定、WWSの商品数量分布分析。
- Minkabuの売買判断・市場営業日・ブローカー固有操作。利用数が多くても分析workflowの責務を広げる根拠にしない。
- notebooksのCorp PPT書式保全、SLQM固有UIやマスター同期、アーカイブ範囲。レポート側の適切な共通化として維持する。
- `query_normalization.py` は共通側で別作業として開発中。今回Shiseido analysis配下の直接利用は見つからず、移行済みとは扱わない。

## 実施単位と完了条件

1. **Sheets batch移行**: Minkabuの小さい送信境界で検証し、Shibuyaへ展開。必要な追加はmegaton側に実装する。
2. **Tags公開API移行**: CSKの進行中変更が落ち着いた箇所から、操作単位で移行する。
3. **CSV比較の抽出**: 共通側の実装・テストと、notebooks/Shiseido両方の呼び出し元移行を一組で行う。

各単位は「共通側追加」「消費側の旧実装削除」「既存挙動の検証」「実行環境への版反映」までで完了とする。
測定するものは削除できた重複処理、private import削減、外部checkout依存削減、誤再送防止。追加した共通API数は成果指標にしない。

調査時、notebooksのrequirementsとMinkabuのuv sourceは `megaton-app v0.31.2` を指定している。ローカルeditableでの成功だけでは固定版の実行環境へ届かない。
5つのanalysis repoのpyprojectにはmegaton-appの直接依存記載がないため、移行時に実際のbootstrap・Python実体・import元・版を確認する。これだけをもって起動不能とは判断しない。

本書の作成にあたり外部API実行・依存更新・利用repoの編集は行っていない。実装移行時には各repoの最新AGENTSと差分を再確認する。

## 第2弾: 共通側の追加実装と消費側ロールアウト(2026-09-12、未リリース)

v0.32.0 の後に共通側へ追加したもの(API 仕様は [REFERENCE.md](REFERENCE.md) 参照):

- `megaton_lib.report_run_summary` — notebooks `lib/report_run_summary.py` からの抽出。`report-run-summary/v1` 契約を保持。差分は `github_run_url(env={})` が process 環境へフォールバックしなくなった点。env 未指定または `None` の場合は従来どおり。notebooks の `build_with_summary()` は呼び出し元の env を転送するため、明示的な空辞書を渡すケースは移行時に確認する。
- Tags `create_rule()` / `create_data_element()` — CSK の webinar provisioning にあった POST payload を公開 API 化。payload は移行元と同一(settings の JSON 区切りのみ差、意味は同一)。
- `scripts/check_consumer_contracts.py` — インストール済み候補版に対して消費repoのオフラインテストを実行し、読み込まれた `megaton_lib` のパスを検証する。

ロールアウト手順(共通側リリース後。それまで消費repoの本番ピンは公開版のまま):

- **notebooks**: 公開ピンを更新し、`lib/report_run_summary.py` を `megaton_lib.report_run_summary` からの明示 import(`SCHEMA_VERSION`, `DEFAULT_TIMEZONE`, `github_run_url`, `now_jst_iso`, `normalize_report_summary`)に置換。既存の呼び出し側 import パスは維持。`tests/test_report_run_summary.py` とレポート/配信系テストをインストール済みリリースに対して実行。
- **CSK**: webinar provisioning の DE/rule 作成 POST を `create_data_element` / `create_rule` に置換し、`response["data"]["id"]` の代わりに `resource["id"]` を読む。既存の apply ゲートは保持。名前・settings・重複選択・library ID・build 判断などのローカル方針は CSK に残す。実行前に no-op/dry-run と作成の両方をテスト。
- `artifacts.box_uploads` → `delivery.box_uploads` エイリアス(モジュール内 TODO 2026-09-30)の削除は、notebooks 全レポートが `delivery` を直接書くようになった後、notebooks ピン更新とセットで行う。
- オフラインテストの成功を Adobe/Sheets への配信成功の証拠と見なさない。契約チェックの一部として remote リソースの publish/build は行わない。
