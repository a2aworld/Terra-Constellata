#!/usr/bin/env python3
"""
Data Ingestion Module for AI Puzzle Pieces Data Pipeline

This module handles CSV data ingestion, including reading, cleaning, validation,
and insertion into the PostgreSQL/PostGIS database.
"""

import csv
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import re
from datetime import datetime

from .connection import PostGISConnection
from .schema import initialize_database

logger = logging.getLogger(__name__)

class PuzzlePiecesIngestion:
    """
    Handles ingestion of puzzle pieces data from CSV files.
    """

    def __init__(self, db: PostGISConnection):
        """
        Initialize the ingestion handler.

        Args:
            db: PostGISConnection instance
        """
        self.db = db
        self.expected_columns = [
            'row_number', 'name', 'entity', 'sub_entity',
            'description', 'source_url', 'latitude', 'longitude'
        ]

    def read_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        Read CSV file into a pandas DataFrame.

        Args:
            file_path: Path to the CSV file

        Returns:
            pandas.DataFrame: Data from the CSV file
        """
        try:
            logger.info(f"Reading CSV file: {file_path}")

            # Read CSV with flexible options
            df = pd.read_csv(
                file_path,
                encoding='utf-8',
                on_bad_lines='skip',  # Skip malformed lines
                low_memory=False
            )

            logger.info(f"Read {len(df)} rows from CSV file")
            return df

        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            raise

    def validate_csv_structure(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate the CSV structure and required columns.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []

        # Check for required columns
        missing_columns = []
        for col in self.expected_columns:
            if col not in df.columns:
                missing_columns.append(col)

        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")

        # Check for empty dataframe
        if df.empty:
            issues.append("CSV file is empty")

        # Check data types
        if 'row_number' in df.columns:
            try:
                pd.to_numeric(df['row_number'], errors='coerce')
            except:
                issues.append("row_number column contains non-numeric values")

        if 'latitude' in df.columns:
            try:
                lat_numeric = pd.to_numeric(df['latitude'], errors='coerce')
                invalid_lat = lat_numeric[(lat_numeric < -90) | (lat_numeric > 90)]
                if not invalid_lat.empty:
                    issues.append(f"Found {len(invalid_lat)} invalid latitude values")
            except:
                issues.append("latitude column contains invalid values")

        if 'longitude' in df.columns:
            try:
                lon_numeric = pd.to_numeric(df['longitude'], errors='coerce')
                invalid_lon = lon_numeric[(lon_numeric < -180) | (lon_numeric > 180)]
                if not invalid_lon.empty:
                    issues.append(f"Found {len(invalid_lon)} invalid longitude values")
            except:
                issues.append("longitude column contains invalid values")

        return len(issues) == 0, issues

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and preprocess the data.

        Args:
            df: Raw DataFrame

        Returns:
            pandas.DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning data...")

        # Create a copy to avoid modifying original
        cleaned_df = df.copy()

        # Clean row_number
        if 'row_number' in cleaned_df.columns:
            cleaned_df['row_number'] = pd.to_numeric(
                cleaned_df['row_number'],
                errors='coerce'
            ).astype('Int64')

        # Clean coordinates
        for coord in ['latitude', 'longitude']:
            if coord in cleaned_df.columns:
                cleaned_df[coord] = pd.to_numeric(
                    cleaned_df[coord],
                    errors='coerce'
                )

        # Clean text fields
        text_columns = ['name', 'entity', 'sub_entity', 'description', 'source_url']
        for col in text_columns:
            if col in cleaned_df.columns:
                # Remove leading/trailing whitespace
                cleaned_df[col] = cleaned_df[col].astype(str).str.strip()

                # Replace empty strings with None
                cleaned_df[col] = cleaned_df[col].replace('', None)

        # Remove rows with missing critical data
        critical_columns = ['row_number', 'name']
        cleaned_df = cleaned_df.dropna(subset=critical_columns)

        # Remove duplicates based on row_number
        if 'row_number' in cleaned_df.columns:
            cleaned_df = cleaned_df.drop_duplicates(subset=['row_number'])

        logger.info(f"Data cleaning completed. Rows: {len(cleaned_df)}")
        return cleaned_df

    def validate_data_quality(self, df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Perform data quality validation.

        Args:
            df: Cleaned DataFrame

        Returns:
            Tuple of (is_valid, list_of_quality_issues)
        """
        quality_issues = []

        # Check for missing values in important columns
        important_columns = ['name', 'latitude', 'longitude']
        for col in important_columns:
            if col in df.columns:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    quality_issues.append({
                        'type': 'missing_values',
                        'column': col,
                        'count': missing_count,
                        'severity': 'WARNING' if missing_count < len(df) * 0.1 else 'ERROR'
                    })

        # Check for coordinate consistency
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Check for (0,0) coordinates which might be placeholders
            zero_coords = ((df['latitude'] == 0) & (df['longitude'] == 0)).sum()
            if zero_coords > 0:
                quality_issues.append({
                    'type': 'zero_coordinates',
                    'count': zero_coords,
                    'severity': 'WARNING'
                })

        # Check for duplicate names
        if 'name' in df.columns:
            duplicate_names = df['name'].duplicated().sum()
            if duplicate_names > 0:
                quality_issues.append({
                    'type': 'duplicate_names',
                    'count': duplicate_names,
                    'severity': 'WARNING'
                })

        # Check URL validity
        if 'source_url' in df.columns:
            invalid_urls = 0
            for url in df['source_url'].dropna():
                if not re.match(r'^https?://', str(url)):
                    invalid_urls += 1
            if invalid_urls > 0:
                quality_issues.append({
                    'type': 'invalid_urls',
                    'count': invalid_urls,
                    'severity': 'WARNING'
                })

        return len([issue for issue in quality_issues if issue['severity'] == 'ERROR']) == 0, quality_issues

    def insert_data_batch(self, df: pd.DataFrame, batch_size: int = 1000) -> Tuple[int, List[str]]:
        """
        Insert data into the database in batches.

        Args:
            df: DataFrame to insert
            batch_size: Number of records per batch

        Returns:
            Tuple of (records_inserted, list_of_errors)
        """
        logger.info(f"Inserting {len(df)} records in batches of {batch_size}")

        records_inserted = 0
        errors = []

        # Prepare the insert query
        insert_query = """
        INSERT INTO puzzle_pieces (
            row_number, name, entity, sub_entity, description,
            source_url, latitude, longitude, geom
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        ON CONFLICT (row_number) DO UPDATE SET
            name = EXCLUDED.name,
            entity = EXCLUDED.entity,
            sub_entity = EXCLUDED.sub_entity,
            description = EXCLUDED.description,
            source_url = EXCLUDED.source_url,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geom = EXCLUDED.geom,
            updated_at = CURRENT_TIMESTAMP
        """

        # Process in batches
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]

            try:
                # Prepare batch data
                batch_data = []
                for _, row in batch_df.iterrows():
                    # Handle potential NaN values
                    lat = row.get('latitude')
                    lon = row.get('longitude')

                    # Skip rows with invalid coordinates
                    if pd.isna(lat) or pd.isna(lon):
                        continue

                    batch_data.append((
                        row.get('row_number'),
                        row.get('name'),
                        row.get('entity'),
                        row.get('sub_entity'),
                        row.get('description'),
                        row.get('source_url'),
                        lat,
                        lon,
                        lon,  # For geom point (longitude first in PostGIS)
                        lat
                    ))

                if batch_data:
                    # Execute batch insert
                    self.db.cursor.executemany(insert_query, batch_data)
                    self.db.connection.commit()
                    records_inserted += len(batch_data)
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_data)} records")

            except Exception as e:
                error_msg = f"Failed to insert batch {i//batch_size + 1}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                self.db.connection.rollback()

        return records_inserted, errors

    def log_processing_result(self, operation: str, status: str,
                            records_processed: int, error_message: Optional[str] = None) -> None:
        """
        Log the processing result to the database.

        Args:
            operation: Operation name
            status: Status (SUCCESS, FAILED, PARTIAL)
            records_processed: Number of records processed
            error_message: Error message if any
        """
        try:
            query = """
            INSERT INTO processing_log (operation, status, records_processed, error_message, completed_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            self.db.execute_command(query, (operation, status, records_processed, error_message))
        except Exception as e:
            logger.error(f"Failed to log processing result: {e}")

    def ingest_csv_file(self, file_path: str, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Complete ingestion process for a CSV file.

        Args:
            file_path: Path to the CSV file
            batch_size: Batch size for insertion

        Returns:
            Dictionary with ingestion results
        """
        result = {
            'success': False,
            'records_processed': 0,
            'records_inserted': 0,
            'errors': [],
            'quality_issues': [],
            'file_path': file_path
        }

        try:
            # Read CSV
            df = self.read_csv_file(file_path)

            # Validate structure
            is_valid, structure_issues = self.validate_csv_structure(df)
            if not is_valid:
                result['errors'].extend(structure_issues)
                result['success'] = False
                return result

            # Clean data
            cleaned_df = self.clean_data(df)

            # Validate data quality
            is_quality_ok, quality_issues = self.validate_data_quality(cleaned_df)
            result['quality_issues'] = quality_issues

            # Insert data
            records_inserted, insert_errors = self.insert_data_batch(cleaned_df, batch_size)
            result['records_processed'] = len(cleaned_df)
            result['records_inserted'] = records_inserted
            result['errors'].extend(insert_errors)

            # Determine success
            result['success'] = len(insert_errors) == 0 and is_quality_ok

            # Log result
            status = 'SUCCESS' if result['success'] else ('PARTIAL' if records_inserted > 0 else 'FAILED')
            self.log_processing_result(
                f"CSV Ingestion: {Path(file_path).name}",
                status,
                records_inserted,
                '; '.join(result['errors']) if result['errors'] else None
            )

            logger.info(f"Ingestion completed: {records_inserted}/{len(cleaned_df)} records inserted")

        except Exception as e:
            error_msg = f"Ingestion failed: {e}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
            result['success'] = False

            # Log failure
            self.log_processing_result(
                f"CSV Ingestion: {Path(file_path).name}",
                'FAILED',
                0,
                error_msg
            )

        return result

def ingest_puzzle_pieces_csv(file_path: str, db_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Convenience function to ingest puzzle pieces from CSV.

    Args:
        file_path: Path to the CSV file
        db_config: Database configuration (optional)

    Returns:
        Dictionary with ingestion results
    """
    # Default database configuration
    if db_config is None:
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'terra_constellata',
            'user': 'postgres',
            'password': ''
        }

    db = PostGISConnection(**db_config)

    with db:
        # Ensure database is initialized
        if not initialize_database(db):
            return {
                'success': False,
                'error': 'Failed to initialize database'
            }

        # Create ingestion handler
        ingestion = PuzzlePiecesIngestion(db)

        # Perform ingestion
        return ingestion.ingest_csv_file(file_path)

if __name__ == "__main__":
    # Test ingestion
    logging.basicConfig(level=logging.INFO)

    # Example usage
    csv_file = "path/to/puzzle_pieces.csv"  # Replace with actual path
    result = ingest_puzzle_pieces_csv(csv_file)

    print(f"Ingestion result: {result}")