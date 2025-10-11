from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Column, Integer, create_engine
import pytest

from sqlalchemy.orm import Session, declarative_base

from shared.account_scope import AccountId, SQLALCHEMY_AVAILABLE, account_id_column


pytestmark = pytest.mark.skipif(
    not SQLALCHEMY_AVAILABLE,
    reason="SQLAlchemy is required to exercise account-scoped persistence",
)


Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(AccountId(), primary_key=True)


class AccountScopedRecord(Base):
    __tablename__ = "account_scoped_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = account_id_column(index=True)


class AccountSnapshot(Base):
    __tablename__ = "account_snapshots"

    account_id = account_id_column(primary_key=True)
    version = Column(Integer, primary_key=True)


def test_account_id_column_configures_foreign_key() -> None:
    column = AccountScopedRecord.__table__.c["account_id"]
    foreign_keys = list(column.foreign_keys)

    assert len(foreign_keys) == 1
    fk = foreign_keys[0]
    assert fk.target_fullname == "accounts.account_id"
    assert isinstance(column.type, AccountId)
    assert column.nullable is False
    assert column.index is True


def test_account_id_column_converts_to_uuid(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path/'scoped.sqlite'}")
    Base.metadata.create_all(engine)

    account_identifier = uuid4()

    with Session(engine) as session:
        record = AccountScopedRecord(account_id=str(account_identifier))
        session.add(record)
        session.commit()

        stored = session.get(AccountScopedRecord, record.id)
        assert stored is not None
        assert isinstance(stored.account_id, UUID)
        assert stored.account_id == account_identifier


def test_account_id_primary_key(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path/'snapshots.sqlite'}")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        snapshot = AccountSnapshot(account_id=uuid4(), version=1)
        session.add(snapshot)
        session.commit()

        loaded = session.get(AccountSnapshot, (snapshot.account_id, snapshot.version))
        assert loaded is not None
        assert loaded.account_id == snapshot.account_id
