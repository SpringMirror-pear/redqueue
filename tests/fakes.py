# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Fake Redis implementations for tests."""


class FakeListRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[bytes]] = {}
        self.values: dict[str, bytes | str] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.commands: list[str] = []
        self.timeouts: list[float | int] = []

    def lpush(self, name: str, *values: bytes) -> int:
        self.commands.append("lpush")
        values_list = self.lists.setdefault(name, [])
        for value in values:
            values_list.insert(0, value)
        return len(values_list)

    def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "RIGHT",
        dest: str = "LEFT",
    ) -> bytes | None:
        self.commands.append("blmove")
        self.timeouts.append(timeout)
        source = self.lists.setdefault(first_list, [])
        if not source:
            return None
        value = source.pop() if src == "RIGHT" else source.pop(0)
        target = self.lists.setdefault(second_list, [])
        if dest == "LEFT":
            target.insert(0, value)
        else:
            target.append(value)
        return value

    def brpoplpush(self, src: str, dst: str, timeout: float) -> bytes | None:
        self.commands.append("brpoplpush")
        self.timeouts.append(timeout)
        source = self.lists.setdefault(src, [])
        if not source:
            return None
        value = source.pop()
        self.lists.setdefault(dst, []).insert(0, value)
        return value

    def lrem(self, name: str, count: int, value: bytes) -> int:
        self.commands.append("lrem")
        values = self.lists.setdefault(name, [])
        for index, item in enumerate(values):
            if item == value:
                del values[index]
                return 1
        return 0

    def lrange(self, name: str, start: int, end: int) -> list[bytes]:
        self.commands.append("lrange")
        values = self.lists.setdefault(name, [])
        stop = None if end == -1 else end + 1
        return values[start:stop]

    def close(self) -> None:
        self.commands.append("close")

    def set(
        self,
        name: str,
        value: bytes | str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None:
        self.commands.append("set")
        if nx and name in self.values:
            return None
        self.values[name] = value
        return True

    def get(self, name: str) -> bytes | str | None:
        self.commands.append("get")
        return self.values.get(name)

    def delete(self, *names: str) -> int:
        self.commands.append("delete")
        removed = 0
        for name in names:
            removed += 1 if self.values.pop(name, None) is not None else 0
        return removed

    def zadd(self, name: str, mapping: dict[str, float]) -> int:
        self.commands.append("zadd")
        zset = self.sorted_sets.setdefault(name, {})
        added = 0
        for member, score in mapping.items():
            added += 0 if member in zset else 1
            zset[member] = score
        return added

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[str]:
        self.commands.append("zrangebyscore")
        minimum = float("-inf") if min == "-inf" else float(min)
        maximum = float("inf") if max == "+inf" else float(max)
        members = [
            member
            for member, score in sorted(
                self.sorted_sets.setdefault(name, {}).items(),
                key=lambda item: item[1],
            )
            if minimum <= score <= maximum
        ]
        begin = start or 0
        end = None if num is None else begin + num
        return members[begin:end]

    def zrem(self, name: str, *values: str) -> int:
        self.commands.append("zrem")
        zset = self.sorted_sets.setdefault(name, {})
        removed = 0
        for value in values:
            removed += 1 if zset.pop(value, None) is not None else 0
        return removed


class FakeAsyncListRedis(FakeListRedis):
    async def lpush(self, name: str, *values: bytes) -> int:
        return super().lpush(name, *values)

    async def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "RIGHT",
        dest: str = "LEFT",
    ) -> bytes | None:
        return super().blmove(first_list, second_list, timeout, src, dest)

    async def brpoplpush(self, src: str, dst: str, timeout: float) -> bytes | None:
        return super().brpoplpush(src, dst, timeout)

    async def lrem(self, name: str, count: int, value: bytes) -> int:
        return super().lrem(name, count, value)

    async def lrange(self, name: str, start: int, end: int) -> list[bytes]:
        return super().lrange(name, start, end)

    async def aclose(self) -> None:
        self.commands.append("aclose")

    async def set(
        self,
        name: str,
        value: bytes | str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None:
        return super().set(name, value, nx=nx, ex=ex, px=px)

    async def get(self, name: str) -> bytes | str | None:
        return super().get(name)

    async def delete(self, *names: str) -> int:
        return super().delete(*names)

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        return super().zadd(name, mapping)

    async def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[str]:
        return super().zrangebyscore(name, min, max, start, num)

    async def zrem(self, name: str, *values: str) -> int:
        return super().zrem(name, *values)


class FakeStreamRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, bytes]]]] = {}
        self.groups: set[tuple[str, str]] = set()
        self.pending: dict[tuple[str, str, str], tuple[str, dict[str, bytes]]] = {}
        self.delivered: set[tuple[str, str, str]] = set()
        self.values: dict[str, bytes | str] = {}
        self.commands: list[str] = []
        self._counter = 0

    def set(
        self,
        name: str,
        value: bytes | str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None:
        self.commands.append("set")
        if nx and name in self.values:
            return None
        self.values[name] = value
        return True

    def get(self, name: str) -> bytes | str | None:
        self.commands.append("get")
        return self.values.get(name)

    def delete(self, *names: str) -> int:
        self.commands.append("delete")
        removed = 0
        for name in names:
            removed += 1 if self.values.pop(name, None) is not None else 0
        return removed

    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = True,
    ) -> bool:
        self.commands.append("xgroup_create")
        key = (name, groupname)
        if key in self.groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        self.groups.add(key)
        self.streams.setdefault(name, [])
        return True

    def xadd(self, name: str, fields: dict[str, bytes | str], id: str = "*") -> str:
        self.commands.append("xadd")
        self._counter += 1
        raw_id = f"{self._counter}-0"
        payload = fields["payload"]
        entry = {"payload": payload if isinstance(payload, bytes) else payload.encode()}
        self.streams.setdefault(name, []).append((raw_id, entry))
        return raw_id

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, bytes]]]]]:
        self.commands.append("xreadgroup")
        stream_key = next(iter(streams))
        entries = self.streams.setdefault(stream_key, [])
        selected = [
            (raw_id, fields)
            for raw_id, fields in entries
            if (stream_key, groupname, raw_id) not in self.delivered
        ][: count or 1]
        for raw_id, fields in selected:
            self.delivered.add((stream_key, groupname, raw_id))
            self.pending[(stream_key, groupname, raw_id)] = (raw_id, fields)
        return [(stream_key, selected)] if selected else []

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.commands.append("xack")
        removed = 0
        for raw_id in ids:
            removed += 1 if self.pending.pop((name, groupname, raw_id), None) else 0
        return removed

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str,
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, bytes]]]]:
        self.commands.append("xautoclaim")
        entries = [
            value
            for (stream, group, _raw_id), value in self.pending.items()
            if stream == name and group == groupname
        ]
        return "0-0", entries[: count or 100]

    def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
    ) -> list[dict[str, str]]:
        self.commands.append("xpending_range")
        return [
            {"message_id": raw_id}
            for (stream, group, raw_id), _value in self.pending.items()
            if stream == name and group == groupname
        ][:count]

    def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        message_ids: list[str],
    ) -> list[tuple[str, dict[str, bytes]]]:
        self.commands.append("xclaim")
        claimed: list[tuple[str, dict[str, bytes]]] = []
        for raw_id in message_ids:
            value = self.pending.get((name, groupname, raw_id))
            if value is not None:
                claimed.append(value)
        return claimed


class FakeAsyncStreamRedis(FakeStreamRedis):
    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = True,
    ) -> bool:
        return super().xgroup_create(name, groupname, id, mkstream)

    async def xadd(
        self,
        name: str,
        fields: dict[str, bytes | str],
        id: str = "*",
    ) -> str:
        return super().xadd(name, fields, id)

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, bytes]]]]]:
        return super().xreadgroup(groupname, consumername, streams, count, block)

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        return super().xack(name, groupname, *ids)

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str,
        count: int | None = None,
    ) -> tuple[str, list[tuple[str, dict[str, bytes]]]]:
        return super().xautoclaim(
            name,
            groupname,
            consumername,
            min_idle_time,
            start_id,
            count,
        )

    async def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
    ) -> list[dict[str, str]]:
        return super().xpending_range(name, groupname, min, max, count)

    async def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        message_ids: list[str],
    ) -> list[tuple[str, dict[str, bytes]]]:
        return super().xclaim(
            name,
            groupname,
            consumername,
            min_idle_time,
            message_ids,
        )


