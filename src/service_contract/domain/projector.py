from __future__ import annotations

from dataclasses import replace

from service_contract.domain.models import (
    ContractEvent,
    ContractLifecycleState,
    ContractState,
    EventType,
    PlanCode,
)


class DomainRuleError(Exception):
    pass


def apply_event(state: ContractState | None, event: ContractEvent) -> ContractState:
    if event.event_type == EventType.APPLICATION_SUBMITTED:
        if state is not None:
            raise DomainRuleError("contract already exists")
        payload = event.payload
        now = event.recorded_at
        return ContractState(
            contract_id=event.contract_id,
            contract_lifecycle_state=ContractLifecycleState.APPLYING,
            current_plan_code=PlanCode(payload["requested_plan_code"]),
            budget_number=payload["budget_number"],
            budget_owner_name=payload["budget_owner_name"],
            api_key_name=payload["api_key_name"],
            team_name=payload["team_name"],
            primary_contact_name=payload["primary_contact_name"],
            secondary_contact_name=payload["secondary_contact_name"],
            department_name=payload["department_name"],
            api_key_id=None,
            service_started_at=None,
            service_ended_at=None,
            last_event_id=event.event_id,
            version_no=1,
            created_at=now,
            updated_at=now,
        )

    if state is None:
        raise DomainRuleError("contract not found")

    if event.event_type == EventType.APPLICATION_APPROVED:
        return _touch(state, event)

    if event.event_type == EventType.APPLICATION_AUTO_APPROVED:
        return _touch(state, event)

    if event.event_type == EventType.APPLICATION_REJECTED:
        return _touch(state, event)

    if event.event_type == EventType.API_KEY_ISSUED:
        if state.contract_lifecycle_state != ContractLifecycleState.APPLYING:
            raise DomainRuleError("api key can only be issued for applying contract")
        next_state = replace(
            state,
            contract_lifecycle_state=ContractLifecycleState.ACTIVE,
            api_key_id=str(event.payload["api_key_id"]),
            service_started_at=event.occurred_at,
        )
        return _touch(next_state, event)

    if event.event_type == EventType.BUDGET_NUMBER_CHANGED:
        if state.contract_lifecycle_state == ContractLifecycleState.TERMINATED:
            raise DomainRuleError("terminated contract cannot be updated")
        next_state = replace(
            state,
            budget_number=str(event.payload["to_budget_number"]),
            budget_owner_name=str(event.payload["to_budget_owner_name"]),
        )
        return _touch(next_state, event)

    if event.event_type == EventType.PLAN_CHANGED:
        if state.contract_lifecycle_state == ContractLifecycleState.TERMINATED:
            raise DomainRuleError("terminated contract cannot be updated")
        next_state = replace(
            state,
            current_plan_code=PlanCode(event.payload["to_plan_code"]),
        )
        return _touch(next_state, event)

    if event.event_type == EventType.CANCELLATION_REQUESTED:
        return _touch(state, event)

    if event.event_type == EventType.CONTRACT_TERMINATED:
        if state.contract_lifecycle_state != ContractLifecycleState.ACTIVE:
            raise DomainRuleError("only active contract can be terminated")
        next_state = replace(
            state,
            contract_lifecycle_state=ContractLifecycleState.TERMINATED,
            service_ended_at=event.occurred_at,
        )
        return _touch(next_state, event)

    raise DomainRuleError(f"unsupported event type: {event.event_type}")


def _touch(state: ContractState, event: ContractEvent) -> ContractState:
    return replace(
        state,
        last_event_id=event.event_id,
        version_no=state.version_no + 1,
        updated_at=event.recorded_at,
    )
