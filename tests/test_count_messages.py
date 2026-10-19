import contextlib
from concurrent.futures import Future
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from confluent_kafka import IsolationLevel, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import OffsetSpec

import count_messages as app


def completed(value):
    future = Future()
    if isinstance(value, BaseException):
        future.set_exception(value)
    else:
        future.set_result(value)
    return future


class FakeAdmin:
    """Replace only Kafka RPCs, using real Future and TopicPartition objects."""

    def __init__(self, offsets=None, metadata_error=None):
        self.offsets = offsets if offsets is not None else {1: (20, 27), 0: (100, 150)}
        self.metadata_error = metadata_error
        self.budgets = []

    def describe_topics(self, topics, *, request_timeout):
        assert topics.topic_names == ["sample"]
        self.budgets.append(request_timeout)
        result = self.metadata_error or SimpleNamespace(
            partitions=[SimpleNamespace(id=p) for p in self.offsets]
        )
        return {"sample": completed(result)}

    def list_offsets(self, requests, *, request_timeout, isolation_level):
        assert isolation_level == IsolationLevel.READ_UNCOMMITTED
        assert {p.partition for p in requests} == set(self.offsets)
        self.budgets.append(request_timeout)
        result = {}
        for partition, spec in reversed(list(requests.items())):
            assert partition.topic == "sample"
            assert spec in (OffsetSpec.earliest(), OffsetSpec.latest())
            offset = self.offsets[partition.partition][0 if spec is OffsetSpec.earliest() else 1]
            value = offset if isinstance(offset, BaseException) else SimpleNamespace(offset=offset)
            result[partition] = completed(value)
        return result


class CountMessagesTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.config = Path(temp.name) / "settings.yml"
        self.config.write_text(
            'kafka:\n  bootstrap.servers: "example.invalid:9094"\n'
            '  security.protocol: SASL_PLAINTEXT\n  sasl.mechanism: PLAIN\n'
            '  sasl.jaas.config: ""\n  username: fixture-user\n'
            '  password: fixture-password\n  topic: sample\n',
            encoding="utf-8",
        )

    def run_cli(self, admin, *args):
        out, err = io.StringIO(), io.StringIO()
        with patch.object(app, "AdminClient", return_value=admin) as factory:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = app.main(["--config", str(self.config), *args])
        return code, out.getvalue(), err.getvalue(), factory

    def test_offsets_are_matched_by_partition_not_future_order(self):
        self.assertEqual(app.get_offsets(FakeAdmin(), "sample", 30), {0: (100, 150), 1: (20, 27)})

    def test_cli_uses_config_and_prints_sorted_partitions_and_total(self):
        code, out, err, factory = self.run_cli(FakeAdmin())
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        lines = [line.split() for line in out.splitlines()]
        self.assertIn(["0", "100", "150", "50"], lines)
        self.assertIn(["1", "20", "27", "7"], lines)
        self.assertLess(lines.index(["0", "100", "150", "50"]), lines.index(["1", "20", "27", "7"]))
        self.assertIn("Estimated total messages: 57", out)
        self.assertIn("sample", out)
        config = factory.call_args.args[0]
        self.assertEqual(config["sasl.username"], "fixture-user")
        self.assertEqual(config["sasl.password"], "fixture-password")
        self.assertNotIn("group.id", config)
        self.assertNotIn("fixture-password", out + err)

    def test_empty_partition_after_retention_succeeds_with_zero(self):
        code, out, err, _ = self.run_cli(FakeAdmin({0: (42, 42)}))
        self.assertEqual(code, 0)
        self.assertIn("Estimated total messages: 0", out)
        self.assertEqual(err, "")

    def test_metadata_errors_produce_failure_without_total(self):
        for error in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.SASL_AUTHENTICATION_FAILED):
            with self.subTest(error=error):
                code, out, err, _ = self.run_cli(FakeAdmin(metadata_error=KafkaException(KafkaError(error))))
                self.assertEqual(code, 1)
                self.assertEqual(out, "")
                self.assertTrue(err)

    def test_one_failed_partition_does_not_print_partial_count(self):
        error = KafkaException(KafkaError(KafkaError._TRANSPORT))
        code, out, err, _ = self.run_cli(FakeAdmin({0: (100, 150), 1: (20, error)}))
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertTrue(err)

    def test_invalid_offsets_fail_instead_of_clamping_or_skipping(self):
        for offsets in ({0: (-1, 5)}, {0: (5, -1)}, {0: (10, 5)}, {}):
            with self.subTest(offsets=offsets):
                code, out, err, _ = self.run_cli(FakeAdmin(offsets))
                self.assertEqual(code, 1)
                self.assertEqual(out, "")
                self.assertTrue(err)

    def test_timeout_in_metadata_or_partition_is_failure(self):
        for admin in (FakeAdmin(metadata_error=TimeoutError()), FakeAdmin({0: (0, TimeoutError())})):
            with self.subTest(admin=admin):
                code, out, err, _ = self.run_cli(admin)
                self.assertEqual(code, 1)
                self.assertEqual(out, "")
                self.assertIn("timeout", err.lower())

    def test_all_rpc_and_future_waits_share_one_deadline(self):
        clock = iter(range(100))
        admin = FakeAdmin()
        real_result = Future.result
        waits = []

        def observe_result(future, timeout=None):
            waits.append(timeout)
            return real_result(future, timeout=timeout)

        with patch.object(app.time, "monotonic", side_effect=lambda: next(clock)):
            with patch.object(Future, "result", observe_result):
                app.get_offsets(admin, "sample", 30)
        self.assertEqual(len(admin.budgets), 3)
        self.assertEqual(len(waits), 5)
        self.assertTrue(all(0 < value < 30 for value in admin.budgets + waits))
        self.assertEqual(admin.budgets, sorted(admin.budgets, reverse=True))
        self.assertEqual(waits, sorted(waits, reverse=True))
        self.assertLess(waits[-1], admin.budgets[0])

    def test_exhausted_deadline_stops_before_rpc(self):
        with patch.object(app.time, "monotonic", side_effect=[0, 31]):
            with self.assertRaises(TimeoutError):
                app.get_offsets(FakeAdmin(), "sample", 30)

    def test_invalid_timeout_is_rejected_before_creating_client(self):
        for value in ("0", "-1", "nan", "inf"):
            with self.subTest(value=value), patch.object(app, "AdminClient") as factory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    app.main(["--timeout", value])
                self.assertEqual(error.exception.code, 2)
                factory.assert_not_called()

    def test_interrupt_returns_130(self):
        code, out, err, _ = self.run_cli(FakeAdmin(metadata_error=KeyboardInterrupt()))
        self.assertEqual(code, 130)
        self.assertEqual(out, "")
        self.assertTrue(err)


if __name__ == "__main__":
    unittest.main()
