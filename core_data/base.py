import abc

import zlib
import json

import msgpack
import redis
import boto
import boto.s3.key
import boto.exception


# TODO: refactor SnapshotPatch
# TODO: create a version factory (think of comparison)
# TODO: implement remove_snapshots_by_versions()
# TODO: implement remove_patches_by_versions()


class BaseSnapshot(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def version(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def payload(self):  # pragma: no cover
        pass

    @abc.abstractmethod
    def make_patch(self, new_snapshot):  # pragma: no cover
        pass

    # TODO: meta (size, time etc)


class BaseSnapshotPatch(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def base_snapshot_version(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def new_snapshot_version(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def payload(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def added(self):  # pragma: no cover
        pass

    @abc.abstractproperty
    def removed(self):  # pragma: no cover
        pass


class BaseCoreDataSnapshotStorage(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def set_latest_version(self):  # pragma: no cover
        pass

    @abc.abstractmethod
    def get_latest_version(self):  # pragma: no cover
        pass

    @abc.abstractmethod
    def set_snapshot_by_version(self, version, snapshot):  # pragma: no cover
        pass

    @abc.abstractmethod
    def get_snapshot_by_version(self, version):  # pragma: no cover
        pass

    @abc.abstractmethod
    def set_patch_by_version(self, version, patch):  # pragma: no cover
        pass

    @abc.abstractmethod
    def get_patch_by_version(self, version):  # pragma: no cover
        pass


class BaseCoreDataSnapshotStorageException(Exception):
    pass


class BasePayloadSerializer(object):
    @abc.abstractmethod
    def pack(self, payload):  # pragma: no cover
        pass

    @abc.abstractmethod
    def unpack(self, packed_payload):  # pragma: no cover
        pass


class BasePayloadSerializerException(Exception):
    pass


class DummyPayloadSerializer(BasePayloadSerializer):
    def pack(self, payload):
        return payload

    def unpack(self, packed_payload):
        return packed_payload


class JsonPayloadSerializer(BasePayloadSerializer):
    def pack(self, payload):
        try:
            packed_payload = json.dumps(payload)
        except:
            raise BasePayloadSerializerException()
        return packed_payload

    def unpack(self, packed_payload):
        try:
            payload = json.loads(packed_payload)
        except:
            raise BasePayloadSerializerException()
        return payload


class MsgPackPayloadSerializer(BasePayloadSerializer):
    def __init__(self, unpack_use_list=False):
        self._unpack_use_list = unpack_use_list

    def pack(self, payload):
        try:
            packed_payload = msgpack.packb(payload)
        except (TypeError, msgpack.PackException) as error:
            raise BasePayloadSerializerException(error)
        return packed_payload

    def unpack(self, packed_payload):
        try:
            payload = msgpack.unpackb(
                packed_payload, encoding='utf-8',
                use_list=self._unpack_use_list)
        except (TypeError, ValueError, msgpack.UnpackException) as error:
            raise BasePayloadSerializerException(error)
        return payload


class ZlibPayloadSerializer(BasePayloadSerializer):
    def pack(self, payload):
        try:
            packed_payload = zlib.compress(payload, 1)
        except (TypeError, zlib.error) as error:
            raise BasePayloadSerializerException(error)
        return packed_payload

    def unpack(self, packed_payload):
        try:
            payload = zlib.decompress(packed_payload)
        except (TypeError, zlib.error) as error:
            raise BasePayloadSerializerException(error)
        return payload


class ChainPayloadSerializer(BasePayloadSerializer):
    def __init__(self, serializers=None):
        self._serializers = list(serializers) if serializers else []

    def pack(self, payload):
        packed_payload = payload
        for serializer in self._serializers:
            packed_payload = serializer.pack(packed_payload)
        return packed_payload

    def unpack(self, packed_payload):
        payload = packed_payload
        for serializer in reversed(self._serializers):
            payload = serializer.unpack(payload)
        return payload


class MsgPackZlibPayloadSerializer(ChainPayloadSerializer):
    def __init__(self, *args, **kwargs):
        serializers = (
            MsgPackPayloadSerializer(*args, **kwargs),
            ZlibPayloadSerializer())
        super(MsgPackZlibPayloadSerializer, self).__init__(
            serializers)


class Snapshot(BaseSnapshot):
    def __init__(self, version, payload):
        self._version = version
        self._payload = payload

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def payload(self):
        return self._payload

    def make_patch(self, new_snapshot):
        pass


class SnapshotPatch(BaseSnapshotPatch):
    def __init__(self, base_snapshot_version, payload):
        self._base_snapshot_version = base_snapshot_version
        self._payload = payload

    @property
    def base_snapshot_version(self):
        return self._base_snapshot_version

    @property
    def new_snapshot_version(self):
        return self._payload['new_snapshot_version']

    @property
    def payload(self):
        return self._payload

    @property
    def added(self):
        return self._payload['added'].iteritems()

    @property
    def removed(self):
        return self._payload['removed']


class DummyCoreDataSnapshotStorage(BaseCoreDataSnapshotStorage):
    def __init__(self):
        self._latest_version = None
        self._snapshots = {}

    def set_latest_version(self, version):
        self._latest_version = version

    def get_latest_version(self):
        try:
            latest_version = int(self._latest_version)
        except (TypeError, ValueError):
            raise BaseCoreDataSnapshotStorageException('Invalid version')
        return latest_version

    def set_snapshot_by_version(self, version, snapshot):
        self._snapshots[version] = snapshot

    def get_snapshot_by_version(self, version):
        if version not in self._snapshots:
            raise BaseCoreDataSnapshotStorageException(
                'Snapshot is not found')
        return self._snapshots[version]

    def set_patch_by_version(self, version, patch):
        raise BaseCoreDataSnapshotStorageException()

    def get_patch_by_version(self, version):
        raise BaseCoreDataSnapshotStorageException()


class RedisCoreDataSnapshotStorage(BaseCoreDataSnapshotStorage):
    def __init__(
            self, redis_conn, snapshots_lock_ttl=5,
            ignore_snapshots_lock_once=True,
            ignore_snapshots_lock_always=False,
            snapshot_factory=Snapshot, snapshot_patch_factory=SnapshotPatch,
            payload_serializer=None):
        self._redis_conn = redis_conn
        self.__redis_lock_script = None

        self._snapshot_factory = snapshot_factory
        self._snapshot_patch_factory = snapshot_patch_factory

        self._payload_serializer = payload_serializer
        # TODO: temporary:
        if self._payload_serializer is None:
            self._payload_serializer = ZlibPayloadSerializer()

        self._snapshots_lock_ttl = snapshots_lock_ttl
        self._ignore_snapshots_lock_once = ignore_snapshots_lock_once
        self._ignore_snapshots_lock_always = ignore_snapshots_lock_always

        self._latest_version_key = 'last_export_date'
        self._snapshot_key = 'core_data'

    @property
    def redis_conn(self):
        return self._redis_conn

    @redis_conn.setter
    def redis_conn(self, redis_conn):
        self._redis_conn = redis_conn

    @property
    def _redis_lock_script(self):
        if self.__redis_lock_script is not None:
            return self.__redis_lock_script

        try:
            self.__redis_lock_script = self._redis_conn.register_script(
                """
                --lockscript, parameters: lock_key, lock_timeout
                local ttl = redis.call('ttl', KEYS[1])
                if ttl <= 0 then
                    return redis.call('setex', KEYS[1], ARGV[1], 'locked');
                end
                return false""")
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

        return self.__redis_lock_script

    def _lock_snapshots(self):
        if self._ignore_snapshots_lock_always:
            return True

        if self._ignore_snapshots_lock_once:
            self._ignore_snapshots_lock_once = False
            return True

        try:
            return self._redis_lock_script(
                keys=('snapshots_lock',),
                args=(self._snapshots_lock_ttl,),
                client=self._redis_conn)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def _clean_version(self, version):
        try:
            return int(version)
        except:
            raise BaseCoreDataSnapshotStorageException('Invalid version')

    def set_latest_version(self, version):
        try:
            version = self._clean_version(version)
            self._redis_conn.set(self._latest_version_key, version)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def get_latest_version(self):
        try:
            version = self._redis_conn.get(self._latest_version_key)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

        return self._clean_version(version)

    def _get_snapshot_key_by_version(self, version):
        return self._snapshot_key

    def _get_patch_key_by_snapshot_version(self, version):
        raise BaseCoreDataSnapshotStorageException()

    def _set_payload_by_key(self, key, payload):
        try:
            packed_payload = self._payload_serializer.pack(payload)
        except BasePayloadSerializerException as error:
            raise BaseCoreDataSnapshotStorageException(error)

        try:
            self._redis_conn.set(key, packed_payload)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def _get_payload_by_key(self, key):
        if not self._lock_snapshots():
            raise BaseCoreDataSnapshotStorageException('locked')

        try:
            packed_payload = self._redis_conn.get(key)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

        try:
            payload = self._payload_serializer.unpack(packed_payload)
        except BasePayloadSerializerException as error:
            raise BaseCoreDataSnapshotStorageException(error)
        return payload

    def set_snapshot_by_version(self, version, snapshot):
        snapshot_key = self._get_snapshot_key_by_version(version)
        self._set_payload_by_key(snapshot_key, snapshot.payload)

    def get_snapshot_by_version(self, version):
        snapshot_key = self._get_snapshot_key_by_version(version)
        snapshot_payload = self._get_payload_by_key(snapshot_key)
        return self._snapshot_factory(version, snapshot_payload)

    def set_patch_by_version(self, version, patch):
        patch_key = self._get_patch_key_by_snapshot_version(version)
        self._set_payload_by_key(patch_key, patch.payload)

    def get_patch_by_version(self, version):
        patch_key = self._get_patch_key_by_snapshot_version(version)
        patch_payload = self._get_payload_by_key(patch_key)
        return self._snapshot_patch_factory(version, patch_payload)


class S3CoreDataSnapshotStorage(BaseCoreDataSnapshotStorage):
    def __init__(
            self, aws_access_key_id=None, aws_secret_access_key=None,
            snapshot_factory=Snapshot, snapshot_patch_factory=SnapshotPatch,
            payload_serializer=None):
        aws_access_key_id = aws_access_key_id
        aws_secret_access_key = aws_secret_access_key
        bucket_name = 'unitcore'

        self._snapshot_factory = snapshot_factory
        self._snapshot_patch_factory = snapshot_patch_factory

        self._payload_serializer = payload_serializer
        # TODO: temporary:
        if self._payload_serializer is None:
            self._payload_serializer = ZlibPayloadSerializer()

        try:
            self._s3_conn = boto.connect_s3(
                aws_access_key_id, aws_secret_access_key)
            self._s3_bucket = self._s3_conn.get_bucket(bucket_name)
        except boto.exception.BotoClientError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def set_latest_version(self):  # pragma: no cover
        pass

    def get_latest_version(self):  # pragma: no cover
        pass

    def _get_snapshot_key_by_version(self, version):
        return 'snapshots/snapshot_%s' % version

    def set_snapshot_by_version(self, version, snapshot):
        payload = snapshot.payload

        try:
            packed_payload = self._payload_serializer.pack(payload)
        except BasePayloadSerializerException as error:
            raise BaseCoreDataSnapshotStorageException(error)

        try:
            s3_key = boto.s3.key.Key(
                self._s3_bucket, self._get_snapshot_key_by_version(version))
            s3_key.set_contents_from_string(packed_payload)
        except boto.exception.BotoClientError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def get_snapshot_by_version(self, version):
        packed_payload = self._get_packed_payload_by_version(version)

        try:
            payload = self._payload_serializer.unpack(packed_payload)
        except BasePayloadSerializerException as error:
            raise BaseCoreDataSnapshotStorageException(error)
        return self._snapshot_factory(version, payload)

    def _get_packed_payload_by_version(self, version):
        try:
            s3_key = boto.s3.key.Key(
                self._s3_bucket, self._get_snapshot_key_by_version(version))
            return s3_key.get_contents_as_string()
        except boto.exception.BotoClientError as error:
            raise BaseCoreDataSnapshotStorageException(error)

    def set_patch_by_version(self, version, patch):  # pragma: no cover
        pass

    def get_patch_by_version(self, version):  # pragma: no cover
        pass


class CompatCoreDataSnapshotStorage(RedisCoreDataSnapshotStorage):
    def __init__(self, *args, **kwargs):
        payload_serializer = kwargs.pop('payload_serializer', None)
        if payload_serializer is None:
            payload_serializer = MsgPackZlibPayloadSerializer()
        kwargs['payload_serializer'] = payload_serializer
        super(CompatCoreDataSnapshotStorage, self).__init__(*args, **kwargs)


class CoreDataSnapshotStorage(CompatCoreDataSnapshotStorage):
    def __init__(self, *args, **kwargs):
        super(CoreDataSnapshotStorage, self).__init__(*args, **kwargs)
        self._latest_version_key = 'snapshot:latest_version'

    def _get_snapshot_key_by_version(self, version):
        return 'snapshot:%s' % version

    def _get_patch_key_by_snapshot_version(self, version):
        return 'snapshot:%s:patch' % version

    def get_all_versions(self):
        try:
            keys_template = self._get_snapshot_key_by_version('*')
            keys = self._redis_conn.keys(keys_template)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)

        versions = set()
        for key in keys:
            try:
                version = key.replace(
                    self._get_snapshot_key_by_version(''), '')
                version = self._clean_version(version)
                versions.add(version)
            except BaseCoreDataSnapshotStorageException:
                pass

        return sorted(versions)

    def remove_snapshots_and_patches_by_versions(self, versions):
        if not versions:
            return

        keys = []
        for version in versions:
            keys.append(self._get_snapshot_key_by_version(version))
            keys.append(self._get_patch_key_by_snapshot_version(version))
        try:
            self._redis_conn.delete(*keys)
        except redis.RedisError as error:
            raise BaseCoreDataSnapshotStorageException(error)
