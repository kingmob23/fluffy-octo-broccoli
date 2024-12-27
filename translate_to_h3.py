import gzip
import json
import logging.handlers
import zipfile
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import h3
from shapely.geometry import Polygon
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()

Path("logs").mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.handlers.RotatingFileHandler(
    "logs/mts_geo_processing.log",
    maxBytes=100 * 1024 * 1024,
    backupCount=10,
    encoding="utf-8",
)
file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)

logger.addHandler(file_handler)
logger.propagate = False


class Sample(Base):
    __tablename__ = "geo_mts"

    id = Column(Integer, primary_key=True)
    city = Column(String)
    business_dt = Column(DateTime)
    sex = Column(String)
    age = Column(String)
    income = Column(String)
    category = Column(String)
    cnt_tourist = Column(Float)
    cnt_workers = Column(Float)
    cnt_livers = Column(Float)
    sum_amount = Column(Float)
    h3_index = Column(String)


def parse_polygon(polygon_str):
    coords_str = polygon_str.strip().lstrip("POLYGON ((").rstrip("))")
    coord_pairs = coords_str.split(", ")
    coords = []
    for pair in coord_pairs:
        lon, lat = map(float, pair.split())
        coords.append((lon, lat))
    polygon = Polygon(coords)
    return polygon


def process_json_file(filepath, session):
    filename = Path(filepath).name
    try:
        logger.info(f"Processing JSON file: {filename}")
        with open(filepath, "r", encoding="utf-8") as f:
            json_objects = [json.loads(line) for line in f]

        total_objects = len(json_objects)
        logger.info(f"[{filename}] Found {total_objects} objects to process")

        processed_count = 0
        error_count = 0

        for obj in json_objects:
            if "cnt_workeкs" in obj:
                obj["cnt_workers"] = obj.pop("cnt_workeкs")

            required_keys = [
                "city",
                "business_dt",
                "sex",
                "age",
                "income",
                "category",
                "cnt_tourist",
                "cnt_workers",
                "cnt_livers",
                "sum_amount",
            ]
            missing_keys = [key for key in required_keys if key not in obj]
            if missing_keys:
                error_count += 1
                continue

            polygon_str = obj["geom62"]
            polygon = parse_polygon(polygon_str)
            centroid = polygon.centroid
            h3_index = h3.latlng_to_cell(centroid.y, centroid.x, 10)

            sample = Sample(
                city=obj["city"],
                business_dt=datetime.strptime(obj["business_dt"], "%Y-%m-%d"),
                sex=obj["sex"],
                age=obj["age"],
                income=obj["income"],
                category=obj["category"],
                cnt_tourist=obj["cnt_tourist"],
                cnt_workers=obj["cnt_workers"],
                cnt_livers=obj["cnt_livers"],
                sum_amount=obj["sum_amount"],
                h3_index=h3_index,
            )

            try:
                session.merge(sample)
                session.commit()
                processed_count += 1

                if processed_count % 1000 == 0:
                    logger.info(
                        f"[{filename}] Processed {processed_count}/{total_objects} records"
                    )

            except IntegrityError as e:
                error_count += 1
                session.rollback()
                continue
            except Exception as e:
                error_count += 1
                logger.error(f"[{filename}] Error adding record to database: {str(e)}")
                session.rollback()
                continue

        logger.info(
            f"[{filename}] File processing completed. "
            f"Successfully processed: {processed_count}, "
            f"Errors: {error_count}, "
            f"Total: {total_objects}"
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error processing file {filepath}. Error details: {str(e)}")
        logger.error("First few lines of file content:")
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                first_lines = [next(f) for _ in range(3)]
                logger.error("\n".join(first_lines))
        except Exception as read_error:
            logger.error(f"Could not read file content: {read_error}")


def cleanup_temp_directory(temp_dir):
    logger.info("Starting cleanup of temporary directory")
    for file in temp_dir.rglob("*"):
        if file.is_file():
            try:
                file.unlink()
            except Exception as e:
                logger.warning(f"Could not delete file {file}: {e}")

    for dir in sorted(temp_dir.rglob("*"), key=lambda x: len(str(x)), reverse=True):
        if dir.is_dir():
            try:
                dir.rmdir()
            except Exception as e:
                logger.warning(f"Could not delete directory {dir}: {e}")

    try:
        temp_dir.rmdir()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.warning(f"Could not delete temp directory: {e}")


def process_gz_file(gz_file, session):
    logger.info(f"Processing gzipped file: {gz_file.name}")
    json_path = gz_file.with_suffix("")

    try:
        with gzip.open(gz_file, "rb") as f_in:
            with open(json_path, "wb") as f_out:
                f_out.write(f_in.read())

        logger.info(f"Decompressed to: {json_path.name}")
        process_json_file(json_path, session)

    finally:
        if json_path.exists():
            json_path.unlink()
            logger.info(f"Deleted decompressed file: {json_path.name}")

        if gz_file.exists():
            gz_file.unlink()
            logger.info(f"Deleted gz file: {gz_file.name}")


def process_single_gz(args):
    gz_file, db_url = args
    engine = create_engine(db_url)
    with Session(engine) as session:
        try:
            process_gz_file(gz_file, session)
            return True, gz_file
        except Exception as e:
            return False, f"Error processing {gz_file}: {str(e)}"


def extract_and_process_nested_archives(main_zip_path):
    logger.info(f"Starting processing of main archive: {main_zip_path}")
    temp_dir = Path("temp_extraction")

    # Create or clean temp directory
    if temp_dir.exists():
        logger.info("Found existing temporary directory, cleaning up")
        cleanup_temp_directory(temp_dir)

    temp_dir.mkdir(exist_ok=True)
    logger.info(f"Created temporary directory: {temp_dir}")

    with zipfile.ZipFile(main_zip_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
        logger.info("Extracted main zip archive")

    try:
        for inner_zip in temp_dir.glob("geo_ekb/*.zip"):
            logger.info(f"Processing inner zip file: {inner_zip.name}")
            with zipfile.ZipFile(inner_zip, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
                logger.info(f"Extracted inner zip: {inner_zip.name}")

        gz_files = list(temp_dir.glob("*.json.gz"))
        logger.info(f"Found {len(gz_files)} .gz files to process")

        if not gz_files:
            logger.warning("No .gz files found to process")
            return

        args = [(gz_file, DB_URL) for gz_file in gz_files]
        num_workers = min(4, len(gz_files))
        logger.info(f"Starting parallel processing with {num_workers} workers")

        with Pool(num_workers) as pool:
            for success, result in pool.imap_unordered(process_single_gz, args):
                if success:
                    logger.info(f"Successfully processed: {result}")
                else:
                    logger.error(result)

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        logger.info("Temporary files preserved for next run")
        raise
    else:
        cleanup_temp_directory(temp_dir)


if __name__ == "__main__":
    logger.info("Script started")
    DB_URL = (
        "postgresql://postgres:Xsy3bDQjJE4Nf7CYkIid@5.188.130.110:5432/nedvision-prod"
    )
    extract_and_process_nested_archives("geo_ekb.zip")
    logger.info("Script completed")
