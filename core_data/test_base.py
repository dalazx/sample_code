import unittest
import mock

import redis

from .base import (
    Snapshot,
    BaseCoreDataSnapshotStorageException,
    RedisCoreDataSnapshotStorage,
    CompatCoreDataSnapshotStorage,
    CoreDataSnapshotStorage,
)


class RedisCoreDataSnapshotStorageTestCase(unittest.TestCase):
    def test_latest_version_redis_error(self):
        redis_conn = mock.Mock()
        redis_conn.get.side_effect = redis.RedisError('')
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_latest_version)

    def test_invalid_latest_version(self):
        redis_conn = mock.Mock()
        redis_conn.get.return_value = None
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_latest_version)

        redis_conn.get.return_value = 'test'
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_latest_version)

    def test_valid_latest_version(self):
        redis_conn = mock.Mock()
        redis_conn.get.return_value = 1
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertEqual(storage.get_latest_version(), 1)

        redis_conn.get.return_value = 2
        self.assertEqual(storage.get_latest_version(), 2)

        redis_conn.get.return_value = '3'
        self.assertEqual(storage.get_latest_version(), 3)

    def test_set_latest_version(self):
        redis_conn = mock.Mock()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.set_latest_version, None)

        redis_conn.set.side_effect = redis.RedisError()
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.set_latest_version, 1)

        redis_conn.set.side_effect = None
        storage.set_latest_version(2)
        self.assertEqual(redis_conn.set.call_args[0][1], 2)

    def test_snapshots_lock_error(self):
        redis_conn = mock.Mock()
        redis_conn.register_script.side_effect = redis.RedisError()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)

        redis_conn.register_script.side_effect = None
        redis_lock_script = mock.Mock()
        redis_lock_script.side_effect = redis.RedisError()
        redis_conn.register_script.return_value = redis_lock_script
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)

    def test_snapshots_locked(self):
        redis_conn = mock.Mock()
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = False
        redis_conn.register_script.return_value = redis_lock_script
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)

    def test_snapshot_error(self):
        redis_conn = mock.Mock()
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = True
        redis_conn.register_script.return_value = redis_lock_script
        redis_conn.get.side_effect = redis.RedisError()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)

        redis_conn.get.side_effect = None
        redis_conn.get.return_value = None
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)

    def test_set_snapshot_by_version_error(self):
        redis_conn = mock.Mock()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        snapshot = Snapshot(1, None)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.set_snapshot_by_version, 1, snapshot)

        redis_conn.set.side_effect = redis.RedisError()
        snapshot = Snapshot(1, 'test')
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.set_snapshot_by_version, 1, snapshot)

    def test_set_get_snapshot_by_version(self):
        redis_conn = mock.Mock()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)

        snapshot = Snapshot(1, 'test')
        storage.set_snapshot_by_version(1, snapshot)
        self.assertTrue(redis_conn.set.called)
        self.assertEqual(len(redis_conn.set.call_args[0]), 2)
        packed_snapshot = redis_conn.set.call_args[0][1]
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = True
        redis_conn.register_script.return_value = redis_lock_script
        redis_conn.get.return_value = packed_snapshot

        snapshot = storage.get_snapshot_by_version(1)
        self.assertEqual(snapshot.version, 1)
        self.assertEqual(snapshot.payload, 'test')

    def test_set_get_snapshot_by_version_ignore_lock(self):
        redis_conn = mock.Mock()
        storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5)

        snapshot = Snapshot(1, 'test')
        storage.set_snapshot_by_version(1, snapshot)
        self.assertTrue(redis_conn.set.called)
        self.assertEqual(len(redis_conn.set.call_args[0]), 2)
        packed_snapshot = redis_conn.set.call_args[0][1]
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = False
        redis_conn.register_script.return_value = redis_lock_script
        redis_conn.get.return_value = packed_snapshot

        snapshot = storage.get_snapshot_by_version(1)
        self.assertEqual(snapshot.version, 1)
        self.assertEqual(snapshot.payload, 'test')
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.get_snapshot_by_version, 1)


class CompatCoreDataSnapshotStorageTestCase(unittest.TestCase):
    def test_set_snapshot_by_version_error(self):
        redis_conn = mock.Mock()
        storage = CompatCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        snapshot = Snapshot(1, object())
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.set_snapshot_by_version, 1, snapshot)

    def test_get_snapshot_by_version_error(self):
        redis_conn = mock.Mock()
        redis_storage = RedisCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        compat_storage = CompatCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = True
        redis_conn.register_script.return_value = redis_lock_script
        redis_conn.get.return_value = 'test'
        snapshot = Snapshot(1, 'test')
        redis_storage.set_snapshot_by_version(1, snapshot)
        packed_snapshot = redis_conn.set.call_args[0][1]
        redis_conn.get.return_value = packed_snapshot
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            compat_storage.get_snapshot_by_version, 1)

    def test_set_get_snapshot_by_version(self):
        redis_conn = mock.Mock()
        storage = CompatCoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)

        snapshot = Snapshot(1, 'test')
        storage.set_snapshot_by_version(1, snapshot)
        self.assertTrue(redis_conn.set.called)
        self.assertEqual(len(redis_conn.set.call_args[0]), 2)
        packed_snapshot = redis_conn.set.call_args[0][1]
        redis_lock_script = mock.Mock()
        redis_lock_script.return_value = True
        redis_conn.register_script.return_value = redis_lock_script
        redis_conn.get.return_value = packed_snapshot

        snapshot = storage.get_snapshot_by_version(1)
        self.assertEqual(snapshot.version, 1)
        self.assertEqual(snapshot.payload, 'test')


class CoreDataSnapshotStorageTestCase(unittest.TestCase):
    def test_get_all_versions(self):
        redis_conn = mock.Mock()
        redis_conn.keys.return_value = [
            'snapshot:5',
            'snapshot:latest_version',
            'snapshot:1',
            'snapshot:test',
            'snapshot:2:patch',
            'snapshot:3'
        ]
        redis_conn.keys.side_effect = redis.RedisError('')
        storage = CoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException, storage.get_all_versions)

        redis_conn.keys.side_effect = None
        self.assertEqual(storage.get_all_versions(), [1, 3, 5])

    def test_remove_snapshots_and_patches_by_versions(self):
        redis_conn = mock.Mock()
        redis_conn.delete.side_effect = redis.RedisError('')
        storage = CoreDataSnapshotStorage(
            redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=False)
        self.assertRaises(
            BaseCoreDataSnapshotStorageException,
            storage.remove_snapshots_and_patches_by_versions, [1, 2, 3])

        redis_conn.delete.side_effect = None
        redis_conn.delete.reset_mock()
        storage.remove_snapshots_and_patches_by_versions([])
        self.assertFalse(redis_conn.delete.called)

        redis_conn.delete.reset_mock()
        storage.remove_snapshots_and_patches_by_versions([1, 2, 3])
        self.assertTrue(redis_conn.delete.called)
