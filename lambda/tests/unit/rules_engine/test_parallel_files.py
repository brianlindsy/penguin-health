"""
Tests for the file-level parallelism in rules_engine_rag.run_validation.

Covers:
- files are processed concurrently (not one-at-a-time)
- queue_counters remain accurate under concurrent bumps
- FILE_WORKERS env var caps the pool size
"""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'),
)


@pytest.fixture
def stub_deps(monkeypatch):
    """Stub every downstream side effect run_validation reaches for.

    Under test we only care about the file dispatch loop — not S3, DDB,
    Bedrock, notifications, or Parquet. Each stub is minimal and
    deterministic.
    """
    import rules_engine_rag as rer

    monkeypatch.setattr(rer, 'build_env_config',
                        lambda org_id: {'BUCKET_NAME': 'bkt'})
    monkeypatch.setattr(rer, 'load_org_rules',
                        lambda org_id: {'rules': [], 'version': 'v'})
    monkeypatch.setattr(rer, 'get_processed_s3_keys',
                        lambda run_id, env_config: set())
    monkeypatch.setattr(rer, 'aggregate_run_summary',
                        lambda run_id, env_config: {
                            'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0})
    monkeypatch.setattr(rer, 'store_run_summary',
                        lambda *a, **kw: None)
    monkeypatch.setattr(rer, 'generate_csv_from_dynamodb',
                        lambda run_id, env_config: ('csv', []))
    monkeypatch.setattr(rer, 'save_csv_to_s3',
                        lambda csv, run_id, env_config: None)
    monkeypatch.setattr(rer, 'save_parquet_to_s3',
                        lambda items, run_id, env_config: None)
    monkeypatch.setattr(rer, '_notify_validation_run_complete',
                        lambda *a, **kw: None)
    monkeypatch.setattr(rer, 'audit_emit', lambda **kw: None)
    return rer


def _make_keys(n):
    return [f'data/2026-05-01/file{i}.csv' for i in range(n)]


class TestParallelFileLoop:
    def test_files_run_concurrently(self, stub_deps, monkeypatch):
        """A blocking process_file must not serialize — 10 files each
        sleeping 100ms should finish in well under 1s, not 1s+."""
        rer = stub_deps
        keys = _make_keys(10)
        monkeypatch.setattr(rer, 'list_data_folder_keys',
                            lambda bucket, date_str: iter(keys))

        in_flight = 0
        max_in_flight = 0
        lock = threading.Lock()

        def fake_process(bucket, key, config, org_id, env_config, run_id,
                         queue_counters=None, counters_lock=None):
            nonlocal in_flight, max_in_flight
            with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            time.sleep(0.1)
            with lock:
                in_flight -= 1

        monkeypatch.setattr(rer, 'process_file', fake_process)

        start = time.time()
        rer.run_validation({
            'organization_id': 'org-1',
            'validation_run_id': 'run-1',
            'dates': ['2026-05-01'],
        })
        elapsed = time.time() - start

        # Serial would take ~1.0s; parallel should finish well under it.
        assert elapsed < 0.5, f"loop appears serial: took {elapsed:.2f}s"
        assert max_in_flight >= 2, \
            f"expected >=2 concurrent workers, saw {max_in_flight}"

    def test_counter_bumps_are_threadsafe(self, stub_deps, monkeypatch):
        """Concurrent counter mutations from many workers must land on
        the exact final total — the lock passed into process_file is the
        contract that makes this true."""
        rer = stub_deps
        keys = _make_keys(200)
        monkeypatch.setattr(rer, 'list_data_folder_keys',
                            lambda bucket, date_str: iter(keys))

        def bumper(bucket, key, config, org_id, env_config, run_id,
                   queue_counters=None, counters_lock=None):
            # Same pattern as the real process_file's guarded bump.
            assert counters_lock is not None
            with counters_lock:
                queue_counters['new_documents'] = \
                    queue_counters.get('new_documents', 0) + 1

        monkeypatch.setattr(rer, 'process_file', bumper)

        result = rer.run_validation({
            'organization_id': 'org-1',
            'validation_run_id': 'run-2',
            'dates': ['2026-05-01'],
        })

        assert result['queue_counters']['new_documents'] == 200

    def test_file_workers_env_var_respected(self, stub_deps, monkeypatch):
        """FILE_WORKERS=1 forces serial execution — a sanity valve for
        rollback without a redeploy."""
        rer = stub_deps
        keys = _make_keys(5)
        monkeypatch.setattr(rer, 'list_data_folder_keys',
                            lambda bucket, date_str: iter(keys))
        monkeypatch.setenv('FILE_WORKERS', '1')

        in_flight = 0
        max_in_flight = 0
        lock = threading.Lock()

        def fake_process(bucket, key, config, org_id, env_config, run_id,
                         queue_counters=None, counters_lock=None):
            nonlocal in_flight, max_in_flight
            with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            time.sleep(0.05)
            with lock:
                in_flight -= 1

        monkeypatch.setattr(rer, 'process_file', fake_process)

        rer.run_validation({
            'organization_id': 'org-1',
            'validation_run_id': 'run-3',
            'dates': ['2026-05-01'],
        })

        assert max_in_flight == 1
