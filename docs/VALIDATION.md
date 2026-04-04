# Validation Guide

`megaton_lib.validation` は Playwright / contract / AA beacon 検証の共通基盤。
analysis repo 側には project 固有の URL、selector、操作手順、期待値、runbook だけを残す。

## Shared-first Rule

- 新しい validation script は direct `sync_playwright()` から書き始めない
- page open / BASIC auth / Tags 差し替え / console-request-response capture / contract 判定 / AA beacon 判定は `megaton_lib.validation` を優先する
- shared helper で表現できない部分だけを local 例外として残す
- local 例外は script 冒頭に理由を書く

## Result Schema

保存する JSON には最低限次を含める。

```json
{
  "executionMode": "live",
  "project": "example-analysis",
  "scenario": "example-check",
  "configPath": "validation/example.local.json",
  "tagsOverride": {
    "launchUrl": "https://assets.adobedtm.com/.../launch-dev.min.js",
    "mode": "auto",
    "envPatterns": ["staging", "development"],
    "exactMatchUrls": [
      "https://assets.adobedtm.com/.../launch-prod.min.js"
    ],
    "abortOldPropertyAssets": false
  }
}
```

補足:

- `executionMode` は少なくとも `live` / `tags_override` を区別する
- GTM preview 実行では `executionMode="gtm_preview"` と `gtmPreview` を残す
- `tagsOverride` は override 実行時だけ入れる
- live と override を同じ filename / payload で混同しない

共通 helper:

- `megaton_lib.validation.build_validation_run_metadata`
- `megaton_lib.validation.build_gtm_preview_override`
- `megaton_lib.validation.describe_tags_launch_override`
- `megaton_lib.validation.describe_gtm_preview_override`
- `megaton_lib.validation.load_auth_profile_store`
- `megaton_lib.validation.resolve_auth_profile`
- `megaton_lib.validation.write_validation_json`

auth profile helper は、1つのローカル JSON に複数のログイン情報や環境別 credential を持たせ、
validation script 側で profile 名だけ切り替えたいときに使う。

## Thin Entrypoint Template

最小テンプレートは [`templates/validation_thin_entrypoint.py`](templates/validation_thin_entrypoint.py) を参照。

基本パターン:

1. local config を読む
2. shared helper で override / metadata を組み立てる
3. `megaton_lib.validation.*` を呼ぶ
4. result に shared metadata を足して保存する

## GTM Preview Rule

- GTM workspace preview は `GtmPreviewOverride` を使う
- 完全自動で preview token を発行するのではなく、Tag Assistant で作成した preview URL または `gtm_auth` / `gtm_preview` を入力として使う
- preview token は repo にコミットしない
- metadata には `containerId` / `previewId` / `cookiesWin` を残し、`authToken` は残さない

## Tags Override Rule

- override を明示した実行は、設定が無い場合に silent fallback させない
- `build_tags_launch_override(..., require=True)` を使う
- production launch URL を dev lib に差し替える場合は `exactMatchUrls` を使う

## Direct Playwright Detection

`sync_playwright()` や raw `page.route()` の増殖を検知するために、次を実行する。

```bash
python scripts/check_validation_usage.py /path/to/repo
```

このチェックは次を報告する。

- direct `sync_playwright()` の使用
- raw `page.route()` / `context.route()` の使用
- `megaton_lib.validation.playwright_pages` / `playwright_capture` を使っていない validation script
- `build_validation_run_metadata` / `write_validation_json` を使っていない validation result の正本 script

`render_*`, `update_*`, snapshot helper, pending-task utility のような周辺 script は
この metadata/save helper 強制の主対象ではない。

検出されたら、まず shared helper へ寄せられないかを確認する。
