# Sheets 正本一本化 — 設計メモ(長期計画)

作成: 2026-08-10(3repo連携監査の後続)。
**進捗: S1+S2+S3a 実施済み(megaton 2.1.0 / app v0.29.0、2026-08-10)** —
stateless スタック全体が `megaton.gsheet_lowlevel` へ移動し、`MegatonGS.call_with_retry`
は同一コアへ委譲。app 側は再エクスポート shim。残: S3b(SheetsService内部の逆転、トリガー待ち)と S4(shim解消、急がない)。
目的: megaton(PyPI)と megaton-app に**並行して存在する2つのSheetsスタック**を、
「実装は1つ・利用形態は2つ」の構造に統合するための段階計画。

## 1. 現状: 二系統の実像(2026-08、megaton 2.0.1 / app v0.28)

| 機能領域 | megaton 側 | megaton-app 側 (`gspread_lowlevel`, 1120行) |
|---|---|---|
| 認証+オープン | `MegatonGS(credentials)` + `open(URL)`(stateful セッション) | `open_spreadsheet(id, SAパス)` → `RetryingSpreadsheet`(stateless 一発) |
| リトライ | `MegatonGS.call_with_retry`(2.0.1で quota-403 パリティ済み) | `call_with_retry` + **ContextVar入れ子抑止** + `Retrying{Spreadsheet,Worksheet}` プロキシ |
| 書き込み | `Sheet.save/append/upsert` + `SheetsService` | `overwrite_worksheet` / `append_rows` / `atomic_replace_dataframe_requests` |
| 読み取り | `sheets.read` / `get_records` | `fetch_worksheet_values` / `cell_data`(数式・serialパース) |
| batchUpdate 部品 | なし | request builder 群(add/delete/update_properties/grid/dimension/copy_format/auto_resize…) |
| A1 / serial | なし | `column_label` / `gs_serial_to_date` |
| セッション設定 | `mg.set.retry`(2.0) | 関数引数 |

**主な消費者**: stateful側 = notebooks全レポート(`tracker.save_sheet`→SheetsService)。
stateless側 = expense(heavy: `call_with_retry`/`cell_value`)、minkabu、notebooks昇格ヘルパー、app `sheets.py`/`report_validation` の一部。

## 2. なぜ2つあるか(そしてなぜそれ自体は正しいか)

- **stateful**(URLで開いてセッション保持)= notebook UX。`mg.open.sheet → mg.save.to.sheet` の流れ。
- **stateless**(spreadsheet_id + SAパスで一発)= スクレイパー/スクリプト/CI。expense・minkabu が要求する形。

**利用形態が2つ必要なのは設計として正しい。** 問題は形態ではなく、
リトライ・request構築・セルパースといった**中身の実装が二重**であること。

## 3. ドリフトの実績(一本化が必要な証拠)

| 時期 | 何が起きたか |
|---|---|
| 2026-06 | app側にのみ retry 追加(quota 30秒フロア)。megaton は後追い |
| 2026-07 | app側にのみ quota-403 リトライ・入れ子抑止・Retryingプロキシ(v0.25)。**megaton側は放置** → notebooksの主要経路(tracker→MegatonGS)だけ quota-403 に脆弱な非対称が3週間存在 |
| 2026-08 | 監査で発見 → megaton 2.0.1 で追いつき |

教訓: **片側の運用障害から得た改善が、もう片側へ自動では届かない。** 手動パリティ維持は既に2回失敗している。

## 4. 目標像(End state)

```
megaton(唯一の実装)
├─ 共有カーネル: リトライ核(1実装) / request builders / cell・serialパーサ
├─ stateless エントリ: open_by_key() / overwrite / append / fetch(純関数群)
└─ stateful エントリ: MegatonGS / SheetsService(内部で stateless 核を呼ぶ)

megaton-app gspread_lowlevel = 再エクスポート shim(+DataFrame糖衣のみ)
```

stateful が stateless 核の上に乗る「逆転」構造 — BQレビューで出した
「statelessを正準化し stateful を薄い殻に」という結論と同型。

## 5. 段階計画

### S1: 純部品の昇格(半日・リスク極小)
移すもの(副作用なし・挙動同一、appは再エクスポート):
- request builders 全部(`add_sheet_request` 〜 `auto_resize_dimensions_request`、`dimension_requests`、`copy_format_request`)
- A1/serial: `column_label` / `gs_serial_to_date`
- セル系: `cell_value` / `cell_data` / `contiguous_runs` / `dataframe_update_cells_rows` / `atomic_replace_dataframe_requests`

新モジュール例: `megaton/gsheet_requests.py`。テストも移管。
効果: request語彙・パーサのドリフトを構造的に根絶。

### S2: リトライ核の統一(1日弱・リスク小)
- megaton に module-level の `sheets_retry(op, func, *, ...)` を新設し、
  現 `MegatonGS.call_with_retry` の実装 + app の **ContextVar入れ子抑止** を吸収
- `MegatonGS.call_with_retry` と app `call_with_retry` は同一核への委譲に
- `Retrying{Spreadsheet,Worksheet}` プロキシも昇格候補
- **パリティテストを megaton 側に置く**(同一例外入力→同一リトライ判定)

効果: 今回型のドリフト(片側だけ判定強化)が構造的に不可能になる。

### S3: I/O操作の一本化(数日+実データ検証・トリガー待ち)
- `open_spreadsheet` / `overwrite_worksheet` / `append_rows` / `fetch_worksheet_values` を megaton へ
- `SheetsService` / `Sheet.save` が内部で同じ書込関数を使う逆転を実施
- 挙動差(USER_ENTERED、min_rows、freeze、ソート)の突合が必要 = **PYTHON_API §6 級の実データ検証必須**
- トリガー: 次のSheets系障害/機能要望、または第3のstateless消費者の出現

### S4(任意): app shim の解消
expense / minkabu / notebooks の import を megaton 直に置換後、shim削除。
shim維持コストはほぼゼロなので**急がない**(タグピンがあるため何も壊れない)。

## 6. 互換・検証・再発防止

- 全消費repoがタグピン → 各段階を megaton マイナー + app パッチとして独立配布可能
- S1/S2 は挙動不変が建前 → 既存テスト移管 + パリティテスト
- S3 のみ出力に影響しうる → 本番コピーへの実走比較(PYTHON_API.md §6 手順)
- **運用規約(即効・コストゼロ)**: 「Sheets系の新機能・修正はまず megaton に書き、app は委譲する」を AGENTS.md に1行 — S1/S2 完了までの暫定ドリフト防止

## 7. 推奨

**S1+S2 をセットで1回の「Sheets統合リリース」(megaton 2.1.0)として実施**するのが費用対効果最大。
次に Sheets 系のバグ修正や機能追加が必要になったタイミングが着手の合図
(単独で予定を切ってやるほどの緊急性はない — 2.0.1 でパリティは回復済みのため)。
S3/S4 は明確なトリガーが来るまで保留。
