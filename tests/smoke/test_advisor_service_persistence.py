from __future__ import annotations

import sys
from typing import List

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from tests.helpers.advisor_service import bootstrap_advisor_service


def _issue_query(module, client: TestClient, *, question: str) -> None:
    store = module.SESSION_STORE
    session = store.create("company")
    response = client.post(
        "/advisor/query",
        headers={"Authorization": f"Bearer {session.token}"},
        json={"user_id": "company", "question": question},
    )
    assert response.status_code == 200


def _stored_questions(module) -> List[str]:
    with module.SessionLocal() as db:
        rows = db.query(module.AdvisorQuery).order_by(module.AdvisorQuery.id).all()
    return [row.question for row in rows]


def test_advisor_queries_persist_across_restarts_and_replicas(tmp_path, monkeypatch) -> None:
    db_filename = "shared-advisor.db"

    module_a = bootstrap_advisor_service(tmp_path, monkeypatch, reset=True, db_filename=db_filename)
    with TestClient(module_a.app) as client_a:
        _issue_query(module_a, client_a, question="What happened overnight?")
    module_a.ENGINE.dispose()

    module_b = bootstrap_advisor_service(tmp_path, monkeypatch, db_filename=db_filename)
    with TestClient(module_b.app) as client_b:
        questions_b = _stored_questions(module_b)
        assert questions_b == ["What happened overnight?"]
        _issue_query(module_b, client_b, question="Summarise current risk exposure")
    persisted = _stored_questions(module_b)
    assert persisted == ["What happened overnight?", "Summarise current risk exposure"]
    module_b.ENGINE.dispose()

    module_c = bootstrap_advisor_service(tmp_path, monkeypatch, db_filename=db_filename)
    replicated_questions = _stored_questions(module_c)
    assert replicated_questions == persisted
    module_c.ENGINE.dispose()
    sys.modules.pop("advisor_service", None)

