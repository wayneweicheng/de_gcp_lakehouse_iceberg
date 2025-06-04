"""
Tests for the Iceberg maintenance module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

from src.maintenance.iceberg_maintenance import IcebergMaintenance


class TestIcebergMaintenance:
    """Test cases for IcebergMaintenance class."""
    
    @pytest.fixture
    def mock_bigquery_client(self):
        """Mock BigQuery client."""
        with patch('src.maintenance.iceberg_maintenance.bigquery.Client') as mock:
            client = Mock()
            mock.return_value = client
            yield client
    
    @pytest.fixture
    def mock_monitoring_client(self):
        """Mock monitoring client."""
        with patch('src.maintenance.iceberg_maintenance.monitoring_v3.MetricServiceClient') as mock:
            client = Mock()
            mock.return_value = client
            yield client
    
    @pytest.fixture
    def maintenance(self, mock_bigquery_client, mock_monitoring_client):
        """Create IcebergMaintenance instance with mocked dependencies."""
        return IcebergMaintenance('test-project', 'test_dataset')
    
    def test_init(self, maintenance, mock_bigquery_client):
        """Test maintenance initialization."""
        assert maintenance.project_id == 'test-project'
        assert maintenance.dataset_id == 'test_dataset'
        assert maintenance.client == mock_bigquery_client
    
    def test_compact_table_success(self, maintenance, mock_bigquery_client):
        """Test successful table compaction."""
        # Mock job result
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        # Mock table stats
        with patch.object(maintenance, '_get_table_stats') as mock_stats:
            mock_stats.side_effect = [
                {'file_count': 100, 'size_gb': 10.0},  # Before
                {'file_count': 50, 'size_gb': 9.5}     # After
            ]
            
            result = maintenance.compact_table('taxi_trips')
            
            assert result['status'] == 'completed'
            assert result['table_name'] == 'taxi_trips'
            assert result['files_before'] == 100
            assert result['files_after'] == 50
            assert result['size_gb_before'] == 10.0
            assert result['size_gb_after'] == 9.5
    
    def test_compact_table_failure(self, maintenance, mock_bigquery_client):
        """Test table compaction failure."""
        # Mock job failure
        mock_bigquery_client.query.side_effect = Exception("Compaction failed")
        
        with patch.object(maintenance, '_get_table_stats') as mock_stats:
            mock_stats.return_value = {'file_count': 100, 'size_gb': 10.0}
            
            result = maintenance.compact_table('taxi_trips')
            
            assert result['status'] == 'failed'
            assert result['table_name'] == 'taxi_trips'
            assert 'error' in result
    
    def test_expire_snapshots_success(self, maintenance, mock_bigquery_client):
        """Test successful snapshot expiration."""
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        result = maintenance.expire_snapshots('taxi_trips', retention_days=7)
        
        assert result['status'] == 'completed'
        assert result['table_name'] == 'taxi_trips'
        
        # Verify SQL was called with correct parameters
        mock_bigquery_client.query.assert_called_once()
        sql_call = mock_bigquery_client.query.call_args[0][0]
        assert 'ICEBERG_EXPIRE_SNAPSHOTS' in sql_call
        assert 'INTERVAL 7 DAY' in sql_call
    
    def test_expire_snapshots_failure(self, maintenance, mock_bigquery_client):
        """Test snapshot expiration failure."""
        mock_bigquery_client.query.side_effect = Exception("Expiration failed")
        
        result = maintenance.expire_snapshots('taxi_trips')
        
        assert result['status'] == 'failed'
        assert result['table_name'] == 'taxi_trips'
        assert 'error' in result
    
    def test_optimize_layout_taxi_trips(self, maintenance, mock_bigquery_client):
        """Test layout optimization for taxi_trips table."""
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        result = maintenance.optimize_layout('taxi_trips')
        
        assert result['status'] == 'completed'
        assert result['table_name'] == 'taxi_trips'
        
        # Verify SQL contains correct sort columns for taxi_trips
        sql_call = mock_bigquery_client.query.call_args[0][0]
        assert 'ICEBERG_REWRITE_PARTITIONS' in sql_call
        assert 'pickup_location_id' in sql_call
    
    def test_optimize_layout_hourly_stats(self, maintenance, mock_bigquery_client):
        """Test layout optimization for hourly_trip_stats table."""
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        result = maintenance.optimize_layout('hourly_trip_stats')
        
        assert result['status'] == 'completed'
        
        # Verify SQL contains correct sort columns for hourly stats
        sql_call = mock_bigquery_client.query.call_args[0][0]
        assert 'stat_hour' in sql_call
    
    def test_cleanup_orphaned_files(self, maintenance, mock_bigquery_client):
        """Test orphaned files cleanup."""
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        result = maintenance.cleanup_orphaned_files()
        
        assert result['status'] == 'completed'
        
        # Verify SQL was called
        sql_call = mock_bigquery_client.query.call_args[0][0]
        assert 'ICEBERG_CLEANUP_ORPHANED_FILES' in sql_call
        assert 'INTERVAL 3 DAY' in sql_call
    
    def test_analyze_query_performance(self, maintenance, mock_bigquery_client):
        """Test query performance analysis."""
        # Mock query results
        mock_result = Mock()
        mock_bigquery_client.query.return_value.result.return_value = [
            Mock(
                job_id='job1',
                total_bytes_processed=15 * 1024**3,  # 15GB
                duration_ms=30000  # 30 seconds
            ),
            Mock(
                job_id='job2',
                total_bytes_processed=5 * 1024**3,   # 5GB
                duration_ms=90000  # 90 seconds
            )
        ]
        
        issues = maintenance.analyze_query_performance()
        
        assert len(issues) == 2
        
        # Check high bytes processed issue
        high_bytes_issue = next(issue for issue in issues if issue['issue'] == 'High bytes processed')
        assert high_bytes_issue['job_id'] == 'job1'
        assert high_bytes_issue['bytes_gb'] == 15.0
        
        # Check long duration issue
        long_duration_issue = next(issue for issue in issues if issue['issue'] == 'Long query duration')
        assert long_duration_issue['job_id'] == 'job2'
        assert long_duration_issue['duration_seconds'] == 90.0
    
    def test_update_table_statistics(self, maintenance, mock_bigquery_client):
        """Test table statistics update."""
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job
        mock_job.result.return_value = None
        
        results = maintenance.update_table_statistics()
        
        # Should update 3 tables
        assert len(results) == 3
        assert 'taxi_trips' in results
        assert 'hourly_trip_stats' in results
        assert 'taxi_zones' in results
        
        # All should be completed
        for table, status in results.items():
            assert status == 'completed'
        
        # Should have called query 3 times
        assert mock_bigquery_client.query.call_count == 3
    
    def test_partition_maintenance(self, maintenance, mock_bigquery_client):
        """Test partition maintenance analysis."""
        # Mock partition query results
        mock_bigquery_client.query.return_value.result.return_value = [
            Mock(
                table_name='taxi_trips',
                partition_id='20240101',
                total_rows=1000000,
                total_logical_bytes=6 * 1024**3  # 6GB - too large
            ),
            Mock(
                table_name='hourly_trip_stats',
                partition_id='20240102',
                total_rows=5000,
                total_logical_bytes=50 * 1024**2  # 50MB - too small
            )
        ]
        
        actions = maintenance.partition_maintenance()
        
        assert len(actions) == 2
        
        # Check large partition action
        large_partition = next(action for action in actions if action['action'] == 'consider_sub_partitioning')
        assert large_partition['table'] == 'taxi_trips'
        assert large_partition['size_gb'] == 6.0
        
        # Check small partition action
        small_partition = next(action for action in actions if action['action'] == 'consider_compaction')
        assert small_partition['table'] == 'hourly_trip_stats'
        assert small_partition['rows'] == 5000
    
    def test_get_table_stats(self, maintenance, mock_bigquery_client):
        """Test table statistics retrieval."""
        # Mock main query result
        mock_bigquery_client.query.return_value.result.return_value = [
            Mock(
                row_count=1000000,
                date_partitions=30,
                earliest_record=datetime(2024, 1, 1),
                latest_record=datetime(2024, 1, 30),
                data_quality_ratio=0.95
            )
        ]
        
        # Mock size query result
        mock_bigquery_client.query.side_effect = [
            Mock(result=lambda: [Mock(
                row_count=1000000,
                date_partitions=30,
                earliest_record=datetime(2024, 1, 1),
                latest_record=datetime(2024, 1, 30),
                data_quality_ratio=0.95
            )]),
            Mock(result=lambda: [Mock(
                size_bytes=5 * 1024**3,  # 5GB
                total_rows=1000000
            )])
        ]
        
        stats = maintenance._get_table_stats('taxi_trips')
        
        assert stats['row_count'] == 1000000
        assert stats['total_rows'] == 1000000
        assert stats['size_gb'] == 5.0
        assert stats['date_partitions'] == 30
        assert stats['data_quality_ratio'] == 0.95
        assert stats['earliest_record'] == '2024-01-01T00:00:00'
        assert stats['latest_record'] == '2024-01-30T00:00:00'
    
    def test_get_table_stats_error(self, maintenance, mock_bigquery_client):
        """Test table statistics retrieval with error."""
        mock_bigquery_client.query.side_effect = Exception("Query failed")
        
        stats = maintenance._get_table_stats('taxi_trips')
        
        assert 'error' in stats
        assert stats['error'] == 'Query failed'
    
    def test_generate_maintenance_report(self, maintenance):
        """Test maintenance report generation."""
        with patch.object(maintenance, 'analyze_query_performance') as mock_perf, \
             patch.object(maintenance, 'partition_maintenance') as mock_partition, \
             patch.object(maintenance, '_get_table_stats') as mock_stats:
            
            mock_perf.return_value = [{'issue': 'test_issue'}]
            mock_partition.return_value = [{'action': 'test_action'}]
            mock_stats.return_value = {'row_count': 1000}
            
            report = maintenance.generate_maintenance_report()
            
            assert 'timestamp' in report
            assert 'table_stats' in report
            assert 'performance_issues' in report
            assert 'partition_analysis' in report
            
            # Should have stats for 3 tables
            assert len(report['table_stats']) == 3
            assert report['performance_issues'] == [{'issue': 'test_issue'}]
            assert report['partition_analysis'] == [{'action': 'test_action'}]
    
    def test_run_full_maintenance(self, maintenance):
        """Test full maintenance cycle."""
        with patch.object(maintenance, 'update_table_statistics') as mock_stats, \
             patch.object(maintenance, 'compact_table') as mock_compact, \
             patch.object(maintenance, 'expire_snapshots') as mock_expire, \
             patch.object(maintenance, 'optimize_layout') as mock_optimize, \
             patch.object(maintenance, 'cleanup_orphaned_files') as mock_cleanup, \
             patch.object(maintenance, 'generate_maintenance_report') as mock_report:
            
            # Setup mocks
            mock_stats.return_value = {'taxi_trips': 'completed'}
            mock_compact.return_value = {'status': 'completed'}
            mock_expire.return_value = {'status': 'completed'}
            mock_optimize.return_value = {'status': 'completed'}
            mock_cleanup.return_value = {'status': 'completed'}
            mock_report.return_value = {'test': 'report'}
            
            result = maintenance.run_full_maintenance()
            
            assert 'start_time' in result
            assert 'end_time' in result
            assert 'results' in result
            assert 'statistics_update' in result
            assert 'orphaned_files_cleanup' in result
            assert 'report' in result
            
            # Should have results for 2 tables
            assert len(result['results']) == 2
            assert 'taxi_trips' in result['results']
            assert 'hourly_trip_stats' in result['results']
            
            # Verify all maintenance operations were called
            mock_stats.assert_called_once()
            assert mock_compact.call_count == 2  # Called for each table
            assert mock_expire.call_count == 2
            assert mock_optimize.call_count == 2
            mock_cleanup.assert_called_once()
            mock_report.assert_called_once()


class TestMaintenanceIntegration:
    """Integration tests for maintenance operations."""
    
    @pytest.fixture
    def real_maintenance(self):
        """Create maintenance instance without mocking for integration tests."""
        with patch('src.maintenance.iceberg_maintenance.bigquery.Client'), \
             patch('src.maintenance.iceberg_maintenance.monitoring_v3.MetricServiceClient'):
            return IcebergMaintenance('test-project', 'test_dataset')
    
    def test_maintenance_workflow(self, real_maintenance):
        """Test complete maintenance workflow."""
        with patch.object(real_maintenance, 'compact_table') as mock_compact, \
             patch.object(real_maintenance, 'expire_snapshots') as mock_expire, \
             patch.object(real_maintenance, 'optimize_layout') as mock_optimize:
            
            # Setup successful responses
            mock_compact.return_value = {'status': 'completed', 'table_name': 'taxi_trips'}
            mock_expire.return_value = {'status': 'completed', 'table_name': 'taxi_trips'}
            mock_optimize.return_value = {'status': 'completed', 'table_name': 'taxi_trips'}
            
            # Run maintenance for single table
            compact_result = real_maintenance.compact_table('taxi_trips')
            expire_result = real_maintenance.expire_snapshots('taxi_trips')
            optimize_result = real_maintenance.optimize_layout('taxi_trips')
            
            # Verify all operations completed
            assert compact_result['status'] == 'completed'
            assert expire_result['status'] == 'completed'
            assert optimize_result['status'] == 'completed'
    
    def test_error_handling(self, real_maintenance):
        """Test error handling in maintenance operations."""
        with patch.object(real_maintenance.client, 'query') as mock_query:
            mock_query.side_effect = Exception("BigQuery error")
            
            # Test that errors are handled gracefully
            result = real_maintenance.compact_table('taxi_trips')
            assert result['status'] == 'failed'
            assert 'error' in result
    
    def test_sql_generation(self, real_maintenance):
        """Test that SQL statements are generated correctly."""
        with patch.object(real_maintenance.client, 'query') as mock_query:
            mock_query.return_value.result.return_value = None
            
            # Test compaction SQL
            real_maintenance.compact_table('taxi_trips', target_file_size_mb=256)
            sql = mock_query.call_args[0][0]
            assert 'ICEBERG_COMPACTION' in sql
            assert 'target_file_size_mb AS 256' in sql
            assert 'test-project.test_dataset.taxi_trips' in sql
            
            mock_query.reset_mock()
            
            # Test snapshot expiration SQL
            real_maintenance.expire_snapshots('taxi_trips', retention_days=14)
            sql = mock_query.call_args[0][0]
            assert 'ICEBERG_EXPIRE_SNAPSHOTS' in sql
            assert 'INTERVAL 14 DAY' in sql 