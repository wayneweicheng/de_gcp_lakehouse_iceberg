"""
Automated maintenance and optimization for BigLake Iceberg tables.
Includes compaction, snapshot management, and performance monitoring.
"""

from google.cloud import bigquery, monitoring_v3
import schedule
import time
from datetime import datetime, timedelta
import logging
import json
import os


class IcebergMaintenance:
    """Automated maintenance for BigLake Iceberg tables."""
    
    def __init__(self, project_id, dataset_id='taxi_dataset'):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.monitoring_client = monitoring_v3.MetricServiceClient()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def compact_table(self, table_name='taxi_trips', target_file_size_mb=128):
        """Compact Iceberg table to optimize file sizes."""
        
        self.logger.info(f"Starting compaction for {table_name}")
        
        # Check table size and file count before compaction
        pre_stats = self._get_table_stats(table_name)
        
        sql = f"""
        CALL BQ.ICEBERG_COMPACTION(
            '{self.project_id}.{self.dataset_id}.{table_name}',
            STRUCT(
                target_file_size_mb AS {target_file_size_mb},
                max_concurrent_file_groups AS 10,
                partition_filter AS "pickup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
            )
        )
        """
        
        try:
            job = self.client.query(sql)
            job.result()
            
            # Check stats after compaction
            post_stats = self._get_table_stats(table_name)
            
            self.logger.info(f"Compaction completed for {table_name}")
            self.logger.info(f"Files reduced from {pre_stats.get('file_count', 'unknown')} to {post_stats.get('file_count', 'unknown')}")
            
            return {
                'table_name': table_name,
                'files_before': pre_stats.get('file_count', 0),
                'files_after': post_stats.get('file_count', 0),
                'size_gb_before': pre_stats.get('size_gb', 0),
                'size_gb_after': post_stats.get('size_gb', 0),
                'status': 'completed'
            }
        except Exception as e:
            self.logger.error(f"Compaction failed for {table_name}: {e}")
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def expire_snapshots(self, table_name='taxi_trips', retention_days=7):
        """Expire old snapshots for Iceberg tables."""
        
        self.logger.info(f"Expiring snapshots older than {retention_days} days for {table_name}")
        
        sql = f"""
        CALL BQ.ICEBERG_EXPIRE_SNAPSHOTS(
            '{self.project_id}.{self.dataset_id}.{table_name}',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {retention_days} DAY)
        )
        """
        
        try:
            job = self.client.query(sql)
            result = job.result()
            
            self.logger.info(f"Snapshot expiration completed for {table_name}")
            return {'status': 'completed', 'table_name': table_name}
        except Exception as e:
            self.logger.error(f"Snapshot expiration failed for {table_name}: {e}")
            return {'status': 'failed', 'table_name': table_name, 'error': str(e)}
    
    def optimize_layout(self, table_name='taxi_trips'):
        """Optimize table layout using Z-ordering based on query patterns."""
        
        self.logger.info(f"Optimizing layout for {table_name}")
        
        # Different optimization strategies based on table
        if table_name == 'taxi_trips':
            sort_columns = ['pickup_location_id', 'pickup_date', 'payment_type']
        elif table_name == 'hourly_trip_stats':
            sort_columns = ['pickup_location_id', 'stat_hour']
        else:
            sort_columns = ['created_at']
        
        sql = f"""
        CALL BQ.ICEBERG_REWRITE_PARTITIONS(
            '{self.project_id}.{self.dataset_id}.{table_name}',
            STRUCT(
                rewrite_strategy AS 'sort',
                sort_columns AS {sort_columns},
                where_clause AS "pickup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)"
            )
        )
        """
        
        try:
            job = self.client.query(sql)
            job.result()
            
            self.logger.info(f"Layout optimization completed for {table_name}")
            return {'status': 'completed', 'table_name': table_name}
        except Exception as e:
            self.logger.error(f"Layout optimization failed for {table_name}: {e}")
            return {'status': 'failed', 'table_name': table_name, 'error': str(e)}
    
    def cleanup_orphaned_files(self):
        """Remove orphaned files from GCS that are no longer referenced."""
        
        self.logger.info("Starting orphaned file cleanup")
        
        sql = f"""
        CALL BQ.ICEBERG_CLEANUP_ORPHANED_FILES(
            '{self.project_id}.{self.dataset_id}.taxi_trips',
            STRUCT(
                older_than AS TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY),
                dry_run AS false
            )
        )
        """
        
        try:
            job = self.client.query(sql)
            result = job.result()
            
            self.logger.info("Orphaned file cleanup completed")
            return {'status': 'completed'}
        except Exception as e:
            self.logger.error(f"Orphaned file cleanup failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def analyze_query_performance(self):
        """Analyze query performance and suggest optimizations."""
        
        # Get recent query stats for taxi tables
        sql = f"""
        SELECT 
          job_id,
          query,
          total_bytes_processed,
          total_slot_ms,
          creation_time,
          end_time,
          TIMESTAMP_DIFF(end_time, creation_time, MILLISECOND) as duration_ms
        FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE statement_type = "SELECT"
          AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND (
            REGEXP_CONTAINS(query, r'taxi_trips|hourly_trip_stats|taxi_zones')
          )
        ORDER BY total_slot_ms DESC
        LIMIT 20
        """
        
        try:
            results = self.client.query(sql).result()
            
            performance_issues = []
            for row in results:
                # Identify potential issues
                if row.total_bytes_processed > 10 * 1024**3:  # > 10GB
                    performance_issues.append({
                        'job_id': row.job_id,
                        'issue': 'High bytes processed',
                        'bytes_gb': row.total_bytes_processed / (1024**3),
                        'suggestion': 'Add partition filters or use clustering columns'
                    })
                
                if row.duration_ms > 60000:  # > 1 minute
                    performance_issues.append({
                        'job_id': row.job_id,
                        'issue': 'Long query duration',
                        'duration_seconds': row.duration_ms / 1000,
                        'suggestion': 'Optimize WHERE clauses and JOINs'
                    })
            
            return performance_issues
        except Exception as e:
            self.logger.error(f"Performance analysis failed: {e}")
            return []
    
    def update_table_statistics(self):
        """Update table statistics for better query planning."""
        
        tables = ['taxi_trips', 'hourly_trip_stats', 'taxi_zones']
        results = {}
        
        for table in tables:
            sql = f"""
            ANALYZE TABLE `{self.project_id}.{self.dataset_id}.{table}`
            """
            
            try:
                job = self.client.query(sql)
                job.result()
                self.logger.info(f"Updated statistics for {table}")
                results[table] = 'completed'
            except Exception as e:
                self.logger.error(f"Failed to update statistics for {table}: {e}")
                results[table] = f'failed: {e}'
        
        return results
    
    def partition_maintenance(self):
        """Perform partition-specific maintenance tasks."""
        
        # Find partitions that need attention
        sql = f"""
        SELECT 
          table_name,
          partition_id,
          total_rows,
          total_logical_bytes,
          last_modified_time
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name IN ('taxi_trips', 'hourly_trip_stats')
          AND partition_id IS NOT NULL
          AND total_rows > 0
        ORDER BY total_logical_bytes DESC
        """
        
        try:
            results = self.client.query(sql).result()
            
            maintenance_actions = []
            for row in results:
                # Check if partition is too large
                size_gb = row.total_logical_bytes / (1024**3)
                
                if size_gb > 5:  # Partition larger than 5GB
                    maintenance_actions.append({
                        'table': row.table_name,
                        'partition': row.partition_id,
                        'action': 'consider_sub_partitioning',
                        'size_gb': size_gb
                    })
                
                # Check if partition is too small
                if row.total_rows < 10000 and size_gb < 0.1:
                    maintenance_actions.append({
                        'table': row.table_name,
                        'partition': row.partition_id,
                        'action': 'consider_compaction',
                        'rows': row.total_rows
                    })
            
            return maintenance_actions
        except Exception as e:
            self.logger.error(f"Partition analysis failed: {e}")
            return []
    
    def generate_maintenance_report(self):
        """Generate comprehensive maintenance report."""
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'table_stats': {},
            'performance_issues': self.analyze_query_performance(),
            'partition_analysis': self.partition_maintenance()
        }
        
        # Get stats for each table
        tables = ['taxi_trips', 'hourly_trip_stats', 'taxi_zones']
        for table in tables:
            report['table_stats'][table] = self._get_table_stats(table)
        
        return report
    
    def _get_table_stats(self, table_name):
        """Get comprehensive table statistics."""
        
        sql = f"""
        SELECT 
          COUNT(*) as row_count,
          COUNT(DISTINCT DATE(pickup_datetime)) as date_partitions,
          MIN(pickup_datetime) as earliest_record,
          MAX(pickup_datetime) as latest_record,
          SUM(CASE WHEN total_amount > 0 THEN 1 ELSE 0 END) / COUNT(*) as data_quality_ratio
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE pickup_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        """
        
        try:
            result = self.client.query(sql).result()
            row = next(result)
            
            # Get table size from information schema
            size_sql = f"""
            SELECT 
              size_bytes,
              row_count as total_rows
            FROM `{self.project_id}.{self.dataset_id}.__TABLES__`
            WHERE table_id = '{table_name}'
            """
            
            size_result = self.client.query(size_sql).result()
            size_row = next(size_result)
            
            return {
                'row_count': row.row_count,
                'total_rows': size_row.total_rows,
                'size_gb': size_row.size_bytes / (1024**3) if size_row.size_bytes else 0,
                'date_partitions': row.date_partitions,
                'earliest_record': row.earliest_record.isoformat() if row.earliest_record else None,
                'latest_record': row.latest_record.isoformat() if row.latest_record else None,
                'data_quality_ratio': float(row.data_quality_ratio) if row.data_quality_ratio else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting stats for {table_name}: {e}")
            return {'error': str(e)}
    
    def run_full_maintenance(self):
        """Run comprehensive maintenance cycle."""
        
        self.logger.info("Starting full maintenance cycle")
        
        maintenance_results = {
            'start_time': datetime.now().isoformat(),
            'results': {}
        }
        
        # Update statistics first
        stats_results = self.update_table_statistics()
        maintenance_results['statistics_update'] = stats_results
        
        # Main table maintenance
        tables = ['taxi_trips', 'hourly_trip_stats']
        
        for table in tables:
            try:
                self.logger.info(f"Maintaining {table}")
                
                # Compact table
                compact_result = self.compact_table(table)
                
                # Expire old snapshots
                snapshot_result = self.expire_snapshots(table, retention_days=7)
                
                # Optimize layout
                layout_result = self.optimize_layout(table)
                
                maintenance_results['results'][table] = {
                    'compaction': compact_result,
                    'snapshot_expiration': snapshot_result,
                    'layout_optimization': layout_result,
                    'status': 'completed'
                }
                
            except Exception as e:
                self.logger.error(f"Error maintaining {table}: {e}")
                maintenance_results['results'][table] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        # Cleanup orphaned files
        cleanup_result = self.cleanup_orphaned_files()
        maintenance_results['orphaned_files_cleanup'] = cleanup_result
        
        # Generate final report
        maintenance_results['end_time'] = datetime.now().isoformat()
        maintenance_results['report'] = self.generate_maintenance_report()
        
        self.logger.info("Full maintenance cycle completed")
        return maintenance_results


def schedule_maintenance(project_id, dataset_id='taxi_dataset'):
    """Setup maintenance schedules."""
    
    maintenance = IcebergMaintenance(project_id, dataset_id)
    
    # Daily full maintenance at 2 AM
    schedule.every().day.at("02:00").do(maintenance.run_full_maintenance)
    
    # Hourly compaction for active partitions during business hours
    schedule.every().hour.do(
        lambda: maintenance.compact_table('taxi_trips')
    ).tag('business_hours')
    
    # Weekly deep optimization on Sundays
    schedule.every().sunday.at("01:00").do(
        lambda: maintenance.optimize_layout('taxi_trips')
    )
    
    return maintenance


def main():
    """Main function for running maintenance."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Iceberg Table Maintenance')
    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--dataset_id', default='taxi_dataset', help='BigQuery dataset ID')
    parser.add_argument('--action', choices=['compact', 'expire', 'optimize', 'cleanup', 'full', 'schedule'], 
                       default='full', help='Maintenance action to perform')
    parser.add_argument('--table_name', default='taxi_trips', help='Table name for specific actions')
    parser.add_argument('--retention_days', type=int, default=7, help='Snapshot retention days')
    
    args = parser.parse_args()
    
    maintenance = IcebergMaintenance(args.project_id, args.dataset_id)
    
    if args.action == 'compact':
        result = maintenance.compact_table(args.table_name)
        print(json.dumps(result, indent=2))
    elif args.action == 'expire':
        result = maintenance.expire_snapshots(args.table_name, args.retention_days)
        print(json.dumps(result, indent=2))
    elif args.action == 'optimize':
        result = maintenance.optimize_layout(args.table_name)
        print(json.dumps(result, indent=2))
    elif args.action == 'cleanup':
        result = maintenance.cleanup_orphaned_files()
        print(json.dumps(result, indent=2))
    elif args.action == 'full':
        result = maintenance.run_full_maintenance()
        print(json.dumps(result, indent=2))
    elif args.action == 'schedule':
        print("Starting maintenance scheduler...")
        schedule_maintenance(args.project_id, args.dataset_id)
        
        while True:
            schedule.run_pending()
            time.sleep(3600)  # Check every hour


if __name__ == '__main__':
    main() 