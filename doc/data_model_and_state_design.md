# データモデル・状態設計（Event/State分離版）

`doc/service_spec.md` を前提に、`イベント` と `状態` を分離して設計する。

## 1. 前提ルール

- 申込・承認・変更・解約はすべて `イベント` として記録する。
- イベントは不変で、`状態` を持たない（`status` カラムを持たない）。
- 契約の現在値は `契約状態` テーブルにのみ保持する。
- 状態遷移は「イベントを適用して状態を更新する」ことで実現する。

## 2. データモデル

## 2.1 マスタ: `plan_master`
- `plan_code` PK (`TRIAL` / `STARTER` / `PRO`)
- `plan_name`
- `monthly_base_fee_yen`
- `token_fee_per_million_yen`
- `daily_token_limit`
- `available_model_scope` (`MINI_ONLY` / `ALL`)
- `approval_required` (Trial=false, その他=true)
- `is_active`
- `created_at`
- `updated_at`

## 2.2 イベント: `contract_event`
- `event_id` PK
- `contract_id`（契約識別子。初回申込時に採番）
- `event_type`
- `occurred_at`（業務上の発生時刻）
- `recorded_at`（記録時刻）
- `actor_type`（`USER` / `ADMIN` / `SYSTEM` / `BATCH`）
- `actor_id` nullable
- `payload_json`（イベント固有データ）
- `correlation_id` nullable（同一処理トレース用）

`event_type` 一覧:
- `APPLICATION_SUBMITTED`（新規申込）
- `APPLICATION_APPROVED`（承認）
- `APPLICATION_REJECTED`（却下）
- `APPLICATION_AUTO_APPROVED`（Trial自動承認）
- `API_KEY_ISSUED`（APIキー払い出し）
- `BUDGET_NUMBER_CHANGED`（予算番号変更）
- `PLAN_CHANGED`（プラン変更）
- `CANCELLATION_REQUESTED`（解約申込）
- `CONTRACT_TERMINATED`（利用終了確定）

補足:
- イベント自体に `state/status` は置かない。
- 承認待ち/承認済み等の概念は、イベント列から導出する。

## 2.3 状態: `contract_state`
- `contract_id` PK
- `contract_lifecycle_state`（`APPLYING` / `ACTIVE` / `TERMINATED`）
- `current_plan_code` FK -> `plan_master.plan_code`
- `budget_number`
- `budget_owner_name`
- `api_key_name`
- `team_name`
- `primary_contact_name`
- `secondary_contact_name`
- `department_name`
- `api_key_id` nullable
- `service_started_at` nullable
- `service_ended_at` nullable
- `last_event_id`（この状態に反映済みの最新イベント）
- `version_no`（楽観ロック用）
- `created_at`
- `updated_at`

役割:
- 現在の契約情報を高速参照するための投影（read/write model）。
- 業務判定は原則 `contract_state` を参照し、監査は `contract_event` を参照。

## 2.4 利用実績: `usage_daily`
- `usage_date`
- `contract_id` FK -> `contract_state.contract_id`
- `model_name`
- `used_tokens`
- `created_at`

制約:
- 複合主キーは `(contract_id, usage_date, model_name)`。

## 2.5 請求明細: `invoice`
- `invoice_id` PK
- `budget_number`
- `title`（見出し）
- `description`（例: `Proプラン Aチーム 2026-03月分`）
- `target_year_month`
- `total_amount_yen`
- `created_at`

## 3. イベント適用による状態遷移

`contract_state.contract_lifecycle_state` のみを状態として扱う。

遷移:
- `APPLICATION_SUBMITTED` 適用時: `APPLYING` を作成
- `API_KEY_ISSUED` 適用時: `APPLYING -> ACTIVE`
- `CONTRACT_TERMINATED` 適用時: `ACTIVE -> TERMINATED`

補助イベント:
- `APPLICATION_APPROVED` / `APPLICATION_AUTO_APPROVED` は遷移前提条件を満たすための事実記録。
- `BUDGET_NUMBER_CHANGED` / `PLAN_CHANGED` はライフサイクル状態を変えず属性のみ更新。
- `CANCELLATION_REQUESTED` は申込事実のみ記録し、実際の終了は `CONTRACT_TERMINATED` で確定。

## 4. イベントpayload例

`APPLICATION_SUBMITTED.payload_json`
- `requested_plan_code`
- `budget_number`
- `budget_owner_name`
- `api_key_name`
- `team_name`
- `primary_contact_name`
- `secondary_contact_name`
- `department_name`

`PLAN_CHANGED.payload_json`
- `from_plan_code`
- `to_plan_code`
- `effective_at`

`BUDGET_NUMBER_CHANGED.payload_json`
- `from_budget_number`
- `to_budget_number`
- `from_budget_owner_name`
- `to_budget_owner_name`
- `effective_at`

`API_KEY_ISSUED.payload_json`
- `api_key_id`
- `issued_at`

`CONTRACT_TERMINATED.payload_json`
- `terminated_at`
- `reason`

## 5. 業務ルール（Event/State分離前提）

- Trial:
  - `APPLICATION_SUBMITTED` 後に `APPLICATION_AUTO_APPROVED` を自動発行。
- 非Trial:
  - `APPLICATION_APPROVED` が発行されるまで `API_KEY_ISSUED` は不可。
- 課金対象:
  - `contract_state.contract_lifecycle_state = ACTIVE` の契約のみ。
- 基本利用料:
  - 日割りなし。対象月に1日でも `ACTIVE` だった契約は満額。
- 請求単位:
  - 予算番号ごとに `invoice` を作成。
  - 明細内訳は持たず、`description` に「XXプラン XXチーム XX月分」を格納。
  - `invoice` は create-only（作成後の更新はしない）。

## 6. 最小実装順

1. `plan_master`, `contract_event`, `contract_state` を実装。
2. `contract_event` 受領時の状態適用ロジック（プロジェクタ）を実装。
3. `usage_daily` と月次請求（`invoice`）を実装。
