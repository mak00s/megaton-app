# Active RepoとAI指示の統一棚卸し

初回調査日: 2026-09-14。以下は**変更前の棚卸しスナップショット**。
その後の実施結果は末尾の「実施結果」を参照する。表のHEAD・依存版・未commit件数は現在値ではない。
共通側の基準はmegaton-app v0.35.0 (`03a5715`)。
本書は時点調査であり、稼働台帳やAPI仕様の正本ではない。次回の編集前にHEADとdirty状態を再確認する。

## 結論

- 31作業場所を確認（深さ3まで30、DMS配下のworktreeを追加して31）。
  本体候補27、別checkout2（notebooks/Colab）、worktree2（megaton-app/DMS）。
- 本体候補27のうち20は90日以内にcommitあり。そのうち外部参照のholoを除く**19をactive候補**とする。
  残る7は活動・稼働要確認。古いcommitだけを根拠に休眠/削除対象にはしない。
- 7repoではGitHub scheduleイベントによる直近runを確認。Shiseidoではローカルlaunchd登録も確認。
  自動実行成功は、その業務成果・外部データの正しさまで検証したという意味ではない。
- **下書き移行の対象はinvoice、Shiseido、notebooksが中心**。Gmail関連の全repoに同じ規則を追加しない。
- rootのAGENTS/CLAUDEが既に薄く分かれているrepoは全面改稿しない。長文化・境界不一致・入口欠落を狙って修正する。

## 調査方法と限界

- 対象: `/Users/maks/Repos`、最大深さ5のGit作業場所。`.git`、node_modules、venv等で探索を打ち切り。
  bare backup、Repos外のcheckout、他ホストは対象外。
- 活動判定: ローカルHEADのcommit日、2026-06-16以降のcommit数、worktreeの差分件数。
  件数はporcelainの行数で、未追跡ディレクトリは1件。作業者や実際の最終編集時刻を示さない。
- `rg --files --hidden`で実装・指示・依存・workflowを探索。資格情報/成果物/キャッシュは除外。
  PythonだけでなくJS/TS/GAS/シェル/Notebookのコードセルも補助検索した。
  検索ヒットは候補であり、コメントやサービス名だけでGmail実利用とは断定しない。
- 主な指示ファイルは内容を確認。その他は存在・行数・見出し・参照先の一次確認。
  ignore対象やより深いnested指示の完全網羅、各AI製品の実際の自動読込挙動は未検証。
- `/Users/maks`、`Repos`、`Repos/github_mak00s`直下にAGENTS/CLAUDEは見つからなかった。
  全repoへ無条件に継承される親AGENTSを今回新設しない。
- GitHubはschedule付き7repoの最新schedule runを読み取った。全workflowの健全性、Cloud Scheduler、
  GASトリガー、他Macのlaunchdは未確認。各repoのfetch/pullや依存インストールはしていない。
- 秘密値、token、メール本文、実データは収集していない。Gmail・Sheets・Boxへのアクセスもしていない。

## Repo別一覧

入口: A=AGENTS.md、C=CLAUDE.md。root以外の例示用指示は入口として数えない。
分類は調査時点の候補であり、移行承認や休眠確定を意味しない。

| 作業場所（Repos/から） | HEAD / 最新commit日 | 90日commit | 未commit件数 | 入口 | 判定・次の対応 |
|---|---|---:|---:|---|---|
| `expense` | `f00109d` / 2026-08-01 | 20 | 0 | A | active候補: 共通browser利用。Gmail追加不要、専用profile/人手確定を維持 |
| `adobe-md` | `e1b47af` / 2026-09-09 | 17 | 0 | A+C | active・定期実行確認: 下書き禁止を維持。581行のAGENTSからレシピ/Gotchasを段階分離 |
| `megaton-notebooks` | `1d3ff70` / 2026-09-12 | 63 | 0 | A | active・定期実行確認: 返信/認証/下書き操作を共通化、pin更新、Gmail境界を追加 |
| `shiseido-dam` | `9b05bb5` / 2026-07-01 | 7 | 0 | C | active候補: CLAUDEだけの汎用ルールをAGENTSへ移設。Gmail追加不要 |
| `shiseido-analysis` | `c178b7d` / 2026-09-14 | 181 | 3 | A+C | active・launchd登録確認: DEI更新/WITH返信を共通化。Sheets所有境界も修正 |
| `browser-test` | `7f0b91e` / 2026-06-02 | 0 | 0 | A | 活動要確認: 90日コミットなし。WWS検証から参照される可能性があり休眠断定しない |
| `wws-analysis` | `64c0a3c` / 2026-09-01 | 18 | 9 | A+C | active候補: 既存薄いCLAUDE維持、browser/共通API入口の差分整理 |
| `shibuya-analysis` | `20de15c` / 2026-09-12 | 62 | 0 | A+C | active・定期実行確認: Sheets所有境界を修正。GAS通知は下書き移行とは分離 |
| `holo-desktop-cli` | `b25df08` / 2026-06-19 | 3 | 1 | なし | 外部参照repo: hcompai由来。examples内指示を自分の共通ルールで上書きしない |
| `scanbe-analysis` | `6859ea8` / 2026-05-28 | 0 | 0 | A | 活動要確認: 90日コミットなし、検証用repo。再利用の必要性を確認後に整備 |
| `aa_ga_diff` | `53fa39f` / 2026-06-10 | 0 | 0 | なし | 稼働要確認: cron_runnerあり。コミット不在だけで休眠扱いしない |
| `ierae-analysis` | `ff74817` / 2026-09-14 | 31 | 0 | A+C | active候補: Sheets所有境界整理。Salesforce/ID接続の個別安全規則を保持 |
| `report-runner` | `761cbdd` / 2026-02-07 | 0 | 4 | なし | 活動要確認: 指示なし・未コミットあり。用途確認が先 |
| `minkabu` | `03f05af` / 2026-09-12 | 225 | 0 | A | active・定期実行確認: browser/取引承認維持、general-purposeという共通層説明を修正 |
| `Colab` | `fe6e150` / 2025-11-05 | 0 | 20 | なし | 別checkout・保留: hs_ai_workflowと同origin、未コミット20件。統合/削除しない |
| `bq-diff` | `995d36b` / 2025-11-04 | 0 | 0 | なし | 活動要確認: 指示なし。実利用確認後、必要なら最小AGENTS |
| `github_mak00s/makoto-shimizu.com` | `2a86e87` / 2026-08-16 | 104 | 0 | なし | active・定期実行確認: root指示なし。公開権限・自動生成物の最小AGENTS候補 |
| `github_mak00s/shibuya_bq` | `9d4d647` / 2025-10-20 | 0 | 8 | なし | 稼働要確認: 旧report repo・未コミット8件。現行Shibuyaと役割確認 |
| `github_mak00s/megaton-notebooks` | `4a9c233` / 2026-08-05 | 47 | 24 | A | 別checkout・保留: 同origin別Git。未コミット24件。先に正本を選び、変更を勝手に移さない |
| `github_mak00s/megaton-app-public-cell-value` | `da1ba9e` / 2026-06-29 | 4 | 0 | A+C | worktree・保留: 本体とgit-common-dir共有。旧branchへ全体ルールを二重適用しない |
| `github_mak00s/megaton` | `9a560a2` / 2026-08-15 | 23 | 0 | なし | active候補: root指示なし。Sheets低レベル所有者として最小AGENTS候補 |
| `github_mak00s/google_cloud_functions` | `a03144c` / 2025-08-26 | 0 | 0 | なし | 稼働要確認: イベント駆動/DTS連携。Cloud側稼働未確認、休眠断定しない |
| `github_mak00s/megaton-app` | `03a5715` / 2026-09-14 | 62 | 0 | A+C | 基準repo: v0.35.0のGmailルール導入済。CLAUDEは既に薄い参照 |
| `github_mak00s/infomart-food` | `c998baf` / 2026-08-13 | 27 | 0 | A+C | active候補・共同repo: 別ownerとの運用境界を維持。BQレシピ等の移設は個別検討 |
| `github_mak00s/hs_ai_workflow` | `339048a` / 2026-08-26 | 13 | 0 | A+C | active・定期実行確認: HubSpot emailイベントでGmailではない。notebook編集規則を保持 |
| `github_mak00s/poimak4` | `904fc31` / 2026-09-05 | 156 | 0 | A | active・定期実行確認: Gmail IMAP取込/Trashは別用途。下書き機能への一括置換は禁止 |
| `github_mak00s/concept-diagram.com` | `ac602d3` / 2026-08-11 | 2 | 0 | なし | active候補: root指示なし。公開サイト用の最小指示を必要性確認後に |
| `dms-analysis` | `a4894fb` / 2026-08-19 | 8 | 0 | A+C | active候補: 本体cleanでもworktreeに作業中データあり。別作業を保護 |
| `csk-analysis` | `749a996` / 2026-09-12 | 106 | 7 | A+C | active候補: 既存bootstrap指示維持、未コミット7件と独立した差分にする |
| `invoice` | `72a39f9` / 2026-09-01 | 15 | 0 | A+C | active候補・下書き優先: 共通client継承済。古いpin/再取得検証/STARRED拡張の境界を整理 |
| `dms-analysis/.claude/worktrees/vibrant-lichterman-5e0e9c` | `a4894fb` / 本体と同commit | 本体と同履歴 | 1 | A+C（本体由来） | worktree。`docs/projects/cx-v2/`が未追跡。別作業として保持 |

## 定期実行の実測

各repoにつき最新1件のscheduleイベントのみ。7件はいずれもcompleted/successだった。

| Repo | 最新schedule runの作成日時 (UTC) | workflow | 結果 / run HEAD |
|---|---|---|---|
| `adobe-md` | 2026-09-13T21:57:24Z | adobe-md daily | success / `e1b47af` |
| `megaton-notebooks` | 2026-09-14T01:07:15Z | Weekly SLQM 70th Report | success / `1d3ff70` |
| `shibuya-analysis` | 2026-09-13T20:36:09Z | Shibuya Dentamap Raw Freshness Monitor | success / `20de15c` |
| `minkabu` | 2026-09-14T01:09:27Z | minkabu-positions | success / `03f05af` |
| `github_mak00s/makoto-shimizu.com` | 2026-09-14T02:37:09Z | Content optimization watchdog | success / `ea55232` |
| `github_mak00s/hs_ai_workflow` | 2026-09-14T03:08:47Z | Weekday Email Event Sync | success / `339048a` |
| `github_mak00s/poimak4` | 2026-09-13T15:16:37Z | daily | success / `5971952` |

- `makoto-shimizu.com`と`poimak4`はrun HEADとローカルHEADが異なる。更新前にremote/default branchとローカルの差を確認する。
- 本ホストの `com.shiseido.with-csv-verify` は定時設定あり、`launchctl print`で登録済み。
  実行成功・現在の検証結果は未確認。expense/poimak4のREADMEにlaunchd手順があっても、このホストでの登録証拠とはしない。
- `hs_ai_workflow/scripts/daily_email_sync.py`はHubSpot配信イベントをBQへ同期する。
  workflow名の「Email」をGmail下書き機能と取り違えない。

## Gmail利用を用途で分ける

| Repo / 実装 | 現在の役割 | 移行方針 |
|---|---|---|
| [invoice/src/gmail_client.py](../../../invoice/src/gmail_client.py) | 共通GmailClientを継承。STARRED解除をローカル拡張 | 下書きは共通の検証付きAPIへ。STARRED操作は別権限・別ポリシーとして残し、下書きAPIを広げない |
| [Shiseido/ops/with_quote.py](../../../shiseido-analysis/ops/with_quote.py) | 共通create_draftによる帳票送付下書き | From照合・再取得検証を既存の帳票確認ゲートに組み込む |
| [Shiseido/ops/with_csv.py](../../../shiseido-analysis/ops/with_csv.py) | 独自返信MIME＋STARRED解除、既存task状態による重複防止 | 返信部分だけ移行。CSV合格条件・task状態・解除操作を保持する |
| [notebooks/lib/with_csv_completion.py](../../../megaton-notebooks/lib/with_csv_completion.py) | 独自返信計画/作成、既存thread draft探索、STARRED解除 | 共通prepare/save/verifyへ。対象メール探索、同thread重複検査、Todoist等の後続処理は利用側 |
| [Shiseido/DEI gmail_apply.py](../../../shiseido-analysis/analysis/dei_monthly_report/gmail_apply.py) | 下書き探索・コメント差込・独自MIME再構築。添付ありは拒否 | 対象選択とコメント生成を保持し、get/update/verifyへ。HTML/添付保持・競合検出が主な改善 |
| [adobe-md/AGENTS.md](../../../adobe-md/AGENTS.md) §1 | Gmail検知＋STARRED付与/解除だけ。下書きも送信も禁止 | **下書き利用ルールを追加しない**。より厳しい既存禁止を保持 |
| [poimak4/utils/gmail_imap_client.py](../../../github_mak00s/poimak4/utils/gmail_imap_client.py) | IMAP読取/メルマガ取込/Trash移動 | OAuth下書き機能とは別用途。自動置換・認証変更をしない |
| [Shibuya/2_fetch.gs](../../../shibuya-analysis/gas/dentamap_raw_fetch/2_fetch.gs) | Dentamap処理の結果通知をMailApp.sendEmailで送る | GAS通知の既存運用を今回の下書き統一で変更しない。必要なら別の承認・通知仕様レビュー |

「送信できるAPIがある」ことと「通常メールをAIが送信してよい」は別。
既存の定期通知やSTARRED操作を発見しただけで削除/停止せず、対象・権限・既存承認を個別に確認する。
`invoice/scripts/send_acclaro_invoice.py`は名前にsendがあるが、確認した処理はcreate_draft。
ファイル名だけで送信実装と判定しない。

### 移行時に維持・再確認する差分

- 共通v0.35.0はReply-To優先、元件名維持、元ToをToへ継承する。
  notebooksの現行計画はFrom優先、Re:付与、元ToをCCへ寄せるため、単なる関数名置換ではない。
  予定宛先・To/CC区分・件名の変更をfixtureで明示する。
- 共通create_draftはFrom=OAuth実アカウントを要求する。送信元aliasの実利用は未確認。
  tokenの中身を棚卸しに掲載せず、移行前の明示的な本人確認で判断する。
- verify失敗でも既存draft IDが残る契約を、invoice/WITHのoperation storeへ正しく保存する。
  タイムアウトで結果不明になったときに、外側のジョブが新規作成を再試行しないことを確認する。
- DEIの添付拒否を単純に解除しない。選択したdraftだけを更新し、本文変更のHTML制限をUI/CLIで説明する。
- 書き込み許可、本文/対象選択、STARRED、freee、Todoist、Box等の後処理は共通ライブラリへ押し込まない。

## 指示ファイルの整理対象

### 維持するもの

- invoice（AGENTS 91行 / CLAUDE 16行）、Shiseido（84/8）、WWS（107/8）、
  Shibuya（90/8）、DMS（64/8）、ierae（78/8）は既に「共通指示＋薄いClaude入口」になっている。
- CSKのCLAUDEはAGENTS全文読込と運用docsへの入口を明示している。消して発見性を落とさない。
- notebooksのローカル開発/GHA運用/ローカルファイル依存の区分と、rawデータの照合規則は残す。
- invoiceの送信禁止・銀行操作の人手確定、CSKのpublish境界、ieraeのID接続制約は、共通化で弱めない。

### 修正する候補

1. **所有境界**: ShiseidoとShibuyaの「新しいSheets helperはmegaton_lib.sheetsへ」、
   ieraeの「新しい汎用Sheets/BQ helperはmegaton_libへ」は広すぎる。
   Sheets低レベルはmegaton、分析workflow統合はmegaton_lib、案件固有処理はconsumerに分ける。
   BQまで機械的にmegatonへ移す意味ではない。
2. **正本の役割**: Shiseido/notebooks等の優先順位表はruntimeの説明として有用だが、
   コードにある操作を安全規則より優先して実行してよい、という解釈を防ぐ。
   「許可/禁止」「現在の実装」「設定/業務定義」を別の正本として説明する。
3. **AGENTSの長文化**: adobe-mdは581行。Hard Rulesと承認済みトリガーの境界を残し、
   モジュール表、JIRAレシピ、時点依存Gotchas、VPN手順を役割別docsへ移す。
   単純な行数削減ではなく、移設先リンクと既存トリガー動作を検証する。
4. **AGENTS未設置**: shiseido-damはCLAUDEだけ。共通事項をAGENTSへ、Claude固有事項だけを残す。
   megaton / makoto-shimizu.com / concept-diagram.comはroot指示なし。用途に即した最小指示を検討する。
5. **文言の過大化**: Minkabuのpyprojectコメントにmegaton-appをgeneral-purpose libとする表現がある。
   現在のarchitecture境界と整合させ、既存のbrowser利用を汎用SaaS拡張の根拠にしない。
6. **Claude重複**: hs_ai_workflowは重要なnotebook編集規則をCLAUDEにも再掲している。
   同時更新が必要な最小要約か、参照だけかを選ぶ。ipynb同期規約を他repoに合わせて消さない。

## 依存宣言とローカル環境を区別する

| Repo | 宣言上のmegaton-app版 | 備考 |
|---|---|---|
| invoice | v0.20.0 | requirements.txt。最初の固定版互換確認候補 |
| notebooks（Repos直下） | v0.33.0 | requirements.txt。GHA導入はpin更新が必要 |
| notebooks（github_mak00s側） | v0.24.0 | 別checkoutで未コミット24件。こちらを自動で追従させない |
| expense | v0.28.0 | requirements.txt。browser用途、Gmailのためだけに更新しない |
| minkabu | v0.31.2 | pyprojectのuv source。更新するならuv lockもセット |
| poimak4 | v0.31.2 | requirements.txt。IMAPは今回の下書き移行対象外 |
| 主要analysis repo | 明示pinは今回の確認範囲で見つからず | 外部checkout/既存Pythonを使う運用がある。これだけで起動不能とは判定しない |

本ホストで調査時に確認したPython:
現行pyenv 3.12.11とinvoice/notebooks/minkabu/ierae/browser-testの既存.venvは、
`find_spec("megaton_lib")`が `Repos/github_mak00s/megaton-app/megaton_lib/__init__.py` を指し、
distribution metadataも0.35.0だった。依存は変更していない。
これはそのPythonでの解決先の確認であり、GHA、他Mac、全CLIの同じ解決先や互換性を保証しない。

## 展開順と完了条件

### 第1段階: 小さいpilot

**invoiceの下書き経路を最初の候補**とする（調査時clean、共通Client継承済）。
次点はShiseidoのwith_quote経路。いずれも帳票作成・送信の実運用は起動せず、まずモックで確認する。

- 新規fixture: expected mailbox不一致、BCC/添付、日本語件名、write成功/readback失敗、結果不明時の重複防止。
- 既存 `invoice/tests/test_draft_plan.py` と `shiseido-analysis/tests/test_with_quote_ops.py` を契約確認の候補にする。
  足りないAPIフェイクを追加し、テスト自身が外部サービスを呼ばないことを先に確認する。
- 隔離環境に公開v0.35.0を導入し、[check_consumer_contracts.py](../scripts/check_consumer_contracts.py)でimport元も検証。
  現行editableだけの成功でpinを更新しない。
- 認証/操作ゲート→共通API呼出し→ID記録→検証失敗の扱いが揃った後、依存宣言とAGENTS/docsを同じrepo単位で更新。
- 実メール検証は対象message/draftとアカウントを別途選択し、明示許可後に下書き作成・再取得まで。送信しない。

### 第2段階: 返信・更新の移行

notebooksのWITH返信とShiseidoのWITH/DEIを操作単位で移行する。
notebooksはローカルと固定版GHAの両経路を検証し、重複防止やSTARREDの契約を保つ。
共通v0.35.0の機能だけで不足する場合は、必要な小さな契約追加をmegaton-app側で先に検討する。

### 第3段階: 指示の整備

残るactive候補を所有境界/入口/安全規則の差分だけ修正する。
Gmail未利用repoに詳細なGmailレシピを増やさない。adobe-mdの下書き禁止など、より狭い規則は維持する。
休眠・別checkout・第三者参照repoは自動変更しない。各repoで差分/リンク/必要なテストを確認し、別々にcommitする。

共通の到達点:
「何を使うか」「何をしてはいけないか」「どこに仕様があるか」が、各repoの入口から分かる。
全ファイルの文面や章番号を同一にすること、共通API数を増やすことは目的にしない。

## 保留事項

- activeは候補判定。7つの旧repoの現行用途、別checkoutの正本、他ホスト/Cloud/GASの稼働は未確定。
- 初回調査時点ではGmail下書き移行・指示変更は未着手。その後の実施範囲は以下のとおり。
- この調査で確認していないAPIへ自動で切り替えたり、既存の定期処理を停止したりしない。

## 実施結果（2026-09-14）

ユーザー承認後、4項目をrepo単位で実施し、ローカルコミットした。push・release・外部サービスの変更はこの整備では行っていない。

| 項目 | Repo | Commit | 実施内容 |
|---|---|---|---|
| Gmail整備の確定 | shiseido-analysis | `b8dc7bd` | 通常メールの共通下書きAPIへの入口・OAuth手順・契約テスト |
| Gmail整備の確定 | invoice | `ad24a9e` | 共通下書き手順・期待アカウント・v0.35.0 pin・契約テスト |
| Gmail整備の確定 | wws-analysis | `673a188` | AGENTS・手順・契約テスト |
| Gmail整備の確定 | shibuya-analysis | `70a5f9d` | AGENTS・手順・契約テスト・Sheets所有境界 |
| Gmail整備の確定 | dms-analysis | `a4b7061` | AGENTS・手順・契約テスト・Gmail出力のignore |
| Gmail整備の確定 | megaton-notebooks | `61c3753` | v0.35.0 pin、期待アカウント、WITH作成前の実アカウント照合、GHA設定とテスト |
| 所有境界の修正 | ierae-analysis | `aeaf065` | Sheets低レベルはmegaton、分析workflow/BQ共通処理はmegaton_lib |
| 所有境界の修正 | minkabu | `2260531` | pyprojectの説明のみ修正。pin・lockは維持 |
| 共通入口の追加 | shiseido-dam | `7504964` | CLAUDEの既存ルールを無変更でAGENTSへ移設、CLAUDEは参照のみ |
| 共通入口の追加 | github_mak00s/megaton | `eca5bd2` | 設計/API正本・Sheets所有境界・テストへの最小入口 |
| 共通入口の追加 | github_mak00s/makoto-shimizu.com | `ca28738` | 編集方針・source/生成物・公開承認の入口。既存作業branch上 |
| 共通入口の追加 | github_mak00s/concept-diagram.com | `075a4cf` | WordPress同期原稿の編集・公開境界。未確認のbuild手順は追加しない |
| 文書分離 | adobe-md | `7df28d5` | AGENTS 581→91行。レシピとGotchasをdocsへ移設、README参照を更新 |

### 検証と維持した境界

- Gmail関連の再検証: Shiseido 7、invoice 77、WWS 2、Shibuya 2、DMS 2、notebooks 25、計115テスト成功。
- adobe-mdのHard Rules・ユーザー対話ルールが元と一致し、移設対象の非空行が全て残ることを機械確認。Gmailは引き続き検知とSTARRED操作のみで、送信・下書き禁止。
- DAMの移設内容が旧CLAUDEと完全一致すること、新規入口とadobe-md文書内のローカルリンク40件の存在を確認。
- 変更差分の空白チェック成功。文書のみのrepoでアプリのbuildや実サービス検証は行っていない。
- ShiseidoのCMP差分とWWSの案件作業をコミット対象から除外。別checkout・worktreeは変更しない。
- Gmailを使わないrepoにGmailレシピは追加しない。GCP projectやSecretを全案件で共通化しない。

### 今回の対象外

既存の帳票専用処理を全てprepare/save/verifyへ置換したわけではない。WITHの業務宛先計画・重複防止・STARRED/Todoist連携、既存レポートの互換create_draft経路を維持した。
全repoのライブOAuth疎通、下書き作成、GHA再実行は今回の検証に含まれない。既存の個別Gmail運用や公開処理を、この文書整備の承認だけで実行しない。
