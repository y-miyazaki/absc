# メンテナンス ノート

## Cron 式解析ロジックの統一化（将来の課題）

### 現状の冗長性
現在、Cron 式マッチング ロジックが 2 つの場所に分散しています：

| 場所                                                  | 関数                  | 用途                                 |
| ----------------------------------------------------- | --------------------- | ------------------------------------ |
| `internal/aws/resources/scheduler_next_invocation.go` | `matchCronPart()`     | EventBridge Scheduler の cron 式評価 |
| `internal/exporter/cron.go`                           | `matchAWSFieldPart()` | HTML 出力用の cron 期間判定          |

両関数のロジックはほぼ同一ですが、パッケージの循環依存を避けるため分散しています：
- `exporter` は `resources` に依存
- `resources` が `exporter` に依存すると循環参照となる

### 推奨される将来の改善案

#### Option 1: 共有モジュール化（推奨）
```
internal/aws/cron/
├── matcher.go       # MatchCronFieldPart() など統一関数
┗── constants.go     # 共有定数
```

利点：
- DRY 原則を満たす
- バグ修正が一箇所
- テストが一元化

実装手順：
1. `internal/aws/cron/` ディレクトリを作成
2. `resources/scheduler_next_invocation.go` の `matchCronPart()` をリファクタリング
3. `exporter/cron.go` からインポートして使用

#### Option 2: 標準ライブラリの活用
Go 1.21+ では `github.com/robfig/cron/v3` や他の高品質なライブラリの採用も検討。

### 定数の分散

#### scheduler_next_invocation.go の定数
```go
const (
    cronDashSeparator = "-"
    cronRangeSplitParts = 2
    cronNoSpecificValue = "?"
    // ...
)
```

#### cron.go の定数
```go
const (
    awsCronDashSeparator = "-"
    awsCronSplitParts = 2
    awsCronNoSpecific = "?"
    // ...色/パターンの定数
)
```

**統一提案:**
- 接頭語を統一（`cronXxx` vs `awsCronXxx`）
- 定数値は同じため、共有ファイルに統合可能

---

## 修正履歴

### 2026-03-19: Lambda maxResults 制限の復活

**問題:** `collect_lambda.go` の `collectLambdaRunsWithPattern()` から maxResults チェックが削除され、CloudWatch Logs が無制限に返される

```go
// 削除されていたコード（復活済み）
if len(runs) >= maxResults {
    return runs, nil
}
```

**影響:**
- Lambda 関数の実行数が多い環境で、メモリ使用量が増加
- 他の収集器との一貫性が低下

**修正:** チェックを復活させ、maxResults 制限を有効化

---

## テスト戦略

### Cron マッチング テスト
- ✅ `TestComputeSchedulerNextInvocation_CronWraparoundMinuteRange`
- ✅ `TestComputeSchedulerNextInvocation_CronWraparoundWeekdayRange`
- ✅ `TestBuildOutput_SlotRunIssues_WraparoundMinuteRange_Issue`
- ✅ `TestBuildOutput_SlotRunIssues_WraparoundWeekdayOnWindowDay_Issue`
- ✅ `TestBuildSlots_CronWraparoundHourRange`
- ✅ `TestBuildSlots_CronWraparoundMinuteStepRange`

### 今後のテスト追加
1. Cron マッチング関数の統一後：`internal/aws/cron/` パッケージの包括的なテスト
2. エッジケース：年の範囲、月の範囲、複数の wrapping range など
