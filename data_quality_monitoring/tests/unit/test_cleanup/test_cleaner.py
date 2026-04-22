"""DataCleaner 单元测试。过期数据自动清理测试
正常清理、部分失败、全失败、保留天数验证、零删除
测试 DataCleaner 的核心逻辑：
1. run — 正常清理流程
2. run — 部分清理失败不影响其他清理
3. run — 全部清理失败
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.dqm.cleanup.cleaner import DataCleaner


@pytest.fixture
def mock_storage():
    return MagicMock()


@pytest.fixture
def mock_snapshot_repo():
    repo = MagicMock()
    repo.cleanup.return_value = 5
    return repo


@pytest.fixture
def mock_check_result_repo():
    repo = MagicMock()
    repo.cleanup.return_value = 10
    return repo


@pytest.fixture
def mock_accuracy_detail_repo():
    repo = MagicMock()
    repo.cleanup.return_value = 3
    return repo


class TestCleanerRun:
    """测试 DataCleaner.run 方法。"""

    @patch("src.dqm.cleanup.cleaner.SnapshotRepository")
    @patch("src.dqm.cleanup.cleaner.CheckResultRepository")
    @patch("src.dqm.cleanup.cleaner.AccuracyDetailRepository")
    def test_run_all_success(
        self, mock_accuracy_cls, mock_check_cls, mock_snapshot_cls,
        mock_storage, mock_snapshot_repo, mock_check_result_repo, mock_accuracy_detail_repo,
    ):
        """三个清理都成功 → 日志输出删除数量。"""
        mock_snapshot_cls.return_value = mock_snapshot_repo
        mock_check_cls.return_value = mock_check_result_repo
        mock_accuracy_cls.return_value = mock_accuracy_detail_repo

        cleaner = DataCleaner(mock_storage)
        cleaner.run()

        mock_snapshot_repo.cleanup.assert_called_once()
        mock_check_result_repo.cleanup.assert_called_once()
        mock_accuracy_detail_repo.cleanup.assert_called_once()

    @patch("src.dqm.cleanup.cleaner.SnapshotRepository")
    @patch("src.dqm.cleanup.cleaner.CheckResultRepository")
    @patch("src.dqm.cleanup.cleaner.AccuracyDetailRepository")
    def test_run_snapshot_cleanup_fails_others_succeed(
        self, mock_accuracy_cls, mock_check_cls, mock_snapshot_cls,
        mock_storage, mock_snapshot_repo, mock_check_result_repo, mock_accuracy_detail_repo,
    ):
        """快照清理失败，其他清理不受影响。"""
        mock_snapshot_repo.cleanup.side_effect = Exception("DB error")
        mock_snapshot_cls.return_value = mock_snapshot_repo
        mock_check_cls.return_value = mock_check_result_repo
        mock_accuracy_cls.return_value = mock_accuracy_detail_repo

        cleaner = DataCleaner(mock_storage)
        cleaner.run()

        # 快照清理失败，但检查结果和明细清理仍应被调用
        mock_snapshot_repo.cleanup.assert_called_once()
        mock_check_result_repo.cleanup.assert_called_once()
        mock_accuracy_detail_repo.cleanup.assert_called_once()

    @patch("src.dqm.cleanup.cleaner.SnapshotRepository")
    @patch("src.dqm.cleanup.cleaner.CheckResultRepository")
    @patch("src.dqm.cleanup.cleaner.AccuracyDetailRepository")
    def test_run_all_cleanup_fail(
        self, mock_accuracy_cls, mock_check_cls, mock_snapshot_cls,
        mock_storage, mock_snapshot_repo, mock_check_result_repo, mock_accuracy_detail_repo,
    ):
        """所有清理都失败 → 不抛异常，继续执行。"""
        mock_snapshot_repo.cleanup.side_effect = Exception("Error 1")
        mock_check_result_repo.cleanup.side_effect = Exception("Error 2")
        mock_accuracy_detail_repo.cleanup.side_effect = Exception("Error 3")
        mock_snapshot_cls.return_value = mock_snapshot_repo
        mock_check_cls.return_value = mock_check_result_repo
        mock_accuracy_cls.return_value = mock_accuracy_detail_repo

        cleaner = DataCleaner(mock_storage)
        # 不应抛异常
        cleaner.run()

        mock_snapshot_repo.cleanup.assert_called_once()
        mock_check_result_repo.cleanup.assert_called_once()
        mock_accuracy_detail_repo.cleanup.assert_called_once()

    @patch("src.dqm.cleanup.cleaner.SnapshotRepository")
    @patch("src.dqm.cleanup.cleaner.CheckResultRepository")
    @patch("src.dqm.cleanup.cleaner.AccuracyDetailRepository")
    def test_run_uses_correct_retention_days(
        self, mock_accuracy_cls, mock_check_cls, mock_snapshot_cls,
        mock_storage, mock_snapshot_repo, mock_check_result_repo, mock_accuracy_detail_repo,
    ):
        """验证清理使用了正确的保留天数配置。"""
        mock_snapshot_cls.return_value = mock_snapshot_repo
        mock_check_cls.return_value = mock_check_result_repo
        mock_accuracy_cls.return_value = mock_accuracy_detail_repo

        cleaner = DataCleaner(mock_storage)
        cleaner.run()

        # 验证调用参数来自 settings.py 的配置
        from config.settings import (
            SNAPSHOT_RETENTION_DAYS,
            CHECK_RESULT_RETENTION_DAYS,
            ACCURACY_DETAIL_RETENTION_DAYS,
        )
        mock_snapshot_repo.cleanup.assert_called_once_with(SNAPSHOT_RETENTION_DAYS)
        mock_check_result_repo.cleanup.assert_called_once_with(CHECK_RESULT_RETENTION_DAYS)
        mock_accuracy_detail_repo.cleanup.assert_called_once_with(ACCURACY_DETAIL_RETENTION_DAYS)

    @patch("src.dqm.cleanup.cleaner.SnapshotRepository")
    @patch("src.dqm.cleanup.cleaner.CheckResultRepository")
    @patch("src.dqm.cleanup.cleaner.AccuracyDetailRepository")
    def test_run_zero_deletions(
        self, mock_accuracy_cls, mock_check_cls, mock_snapshot_cls,
        mock_storage, mock_snapshot_repo, mock_check_result_repo, mock_accuracy_detail_repo,
    ):
        """没有过期数据时，deleted=0。"""
        mock_snapshot_repo.cleanup.return_value = 0
        mock_check_result_repo.cleanup.return_value = 0
        mock_accuracy_detail_repo.cleanup.return_value = 0
        mock_snapshot_cls.return_value = mock_snapshot_repo
        mock_check_cls.return_value = mock_check_result_repo
        mock_accuracy_cls.return_value = mock_accuracy_detail_repo

        cleaner = DataCleaner(mock_storage)
        cleaner.run()

        # 应该仍然调用 cleanup（即使没有数据要清理）
        mock_snapshot_repo.cleanup.assert_called_once()
        mock_check_result_repo.cleanup.assert_called_once()
        mock_accuracy_detail_repo.cleanup.assert_called_once()
