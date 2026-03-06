# Audit Project Configs

`megaton-app` では、監査の共通機能（1-9）だけをここで扱います。
案件固有のロジック（10-12）は各プロジェクト repo 側で拡張してください。

## 共通機能（1-9）

1. 監査設定モデル（project config）
2. 監査ランナー（共通オーケストレーション）
3. GTM 設定抽出
4. Adobe Tags 設定抽出
5. GA4 データ抽出（site/host/session）
6. AA データ抽出（site dimension + metric）
7. site mapping 監査（設定値 vs 実データ）
8. JSON/CSV レポート出力
9. 共通 CLI (`scripts/audit.py`)

## 案件固有（10-12）

10. ドメイン固有の正規化・補正ルール
11. 個別シート設計/書き込み要件
12. 特定KPIの閾値・アラート運用

## 実行例

```bash
python scripts/audit.py site-mapping --project example --config-root configs/audit/projects --output output/audit
python scripts/audit.py export-tag-config --project example --config-root configs/audit/projects --output output/audit
```
