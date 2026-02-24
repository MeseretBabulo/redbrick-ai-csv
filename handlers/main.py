import logging
import os
import glob
import pandas as pd
from datetime import datetime
from io import StringIO
from google.cloud import storage

rds_dir = "json/"
csv_dir = "csv/"
csv_dir_archive = "csv/archive"

# Optional GCS configuration for Cloud Run execution.
# If INPUT_BUCKET/OUTPUT_BUCKET are not set, local folders are used.
INPUT_BUCKET = os.getenv("INPUT_BUCKET", "").strip()
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "").strip()
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "").strip("/")  
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "").strip("/")  
PROJECT_FOLDER_NAME = os.getenv("PROJECT_FOLDER_NAME", "").strip()


def empty_data(row):
    """This will get the data for no nodules"""
    rows = []
    data = data_values() 
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if row.get("status"):
        data["Stage"] = row.get("currentStageName")
    rows.append(data)
    return rows


def check_data_to_be_flagged_for_no_nodule(data):
    """Check data to be flagged"""
    data["Classification (Study Reviewed?)"]
    
    if data["Classification (Study Reviewed?)"] == "----":
        data["Flagged"] += "Missing Classifications,"

    return data    


def no_nodule(row, task, classification, data):
    """Data from No Consensus"""
    """This builds a row for tasks where:
        there are no landmarks3d nodules, but classification exists
        """
    rows = []
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if task.get("updatedBy"):
        data["Clinician Name"] = task.get("updatedBy")
    if task.get("updatedAt"):
        date = datetime.fromisoformat(task.get("updatedAt"))
        formatted_str = date.strftime('%Y:%m:%d %H:%M:%S')
        data["Updated At"] = formatted_str
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if task.get("status"):
        data["Status"] = task.get("status")
    
    if classification:
        attributes = classification.get("attributes")
        if attributes:
            if attributes.get("Study Reviewed?"):
                data["Classification (Study Reviewed?)"] = attributes.get("Study Reviewed?")
            if attributes.get("Case-wise LungRADS Score"):
                data["Classification (Case-wise LungRADS Score)"] = attributes.get("Case-wise LungRADS Score")
            if attributes.get("Confidence on LungRADS Score"):
                data["Classification (Confidence on LungRADS Score)"] = attributes.get("Confidence on LungRADS Score")
            if attributes.get("Comments on LungRADS Score"):
                data["Classification (Comments on LungRADS Score)"] = attributes.get("Comments on LungRADS Score")
    
    flagged_data = check_data_to_be_flagged_for_no_nodule(data)
    rows.append(flagged_data)
    return rows


def check_data_to_be_flagged(data):
    """Check if Data has something to be flagged"""
    attributes = [
        data["Nodule Location"],
        data["Nodule Type"],
        data["Confidence on Nodule Type"],
        data["Nodule Morphology"],
        data["Confidence on Nodule Morphology"],
        data["Nodule-wise LungRADS Score"],
        data["Confidence on LungRADS Score"],
    ]
    classification = [
        data["Classification (Study Reviewed?)"],
        data["Classification (Case-wise LungRADS Score)"],
        data["Classification (Confidence on LungRADS Score)"],
    ]

    part_solid = [
        data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"],
        data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"],
        data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"],
    ]

    static = [
        data["Nodule Volume 2D Mean Diameter"],
        data["Nodule Volume 2D Max Diameter"],
        data["Nodule Volume 2D Min Diameter"],
    ]

    if data["Nodule Location"] == "----" and data["Nodule Suspicion Rank (1-5)"]  != "----":
        data["Flagged"] += "Unnecessary Rank,"
    
    if "----" in attributes and data["Nodule Location"] != "----":
        data["Flagged"] += "Missing Attributes,"
        
    if "----" in classification:
        data["Flagged"] += "Missing Classifications,"
        
    if data["Nodule Type"] == "Part-solid":
        if "----" in part_solid:
            data["Flagged"] += "Missing Part-solid Data,"

    if data["Nodule Location"] != "----":
        if "----" in static:
            data["Flagged"] += "Missing Measure of Center,"
            
    if data["Nodule Suspicion Rank (1-5)"] == "1":
        if data["Classification (Case-wise LungRADS Score)"].split(" ")[0] != data["Nodule-wise LungRADS Score"].split(" ")[0]:
            data["Flagged"] += "LungRADS Score Mismatch,"

    return data


def get_task_data(row, task, nodule, volume_measures, classification, data):
    """Data from Super Task"""
    """main extractor for one nodule."""
    """Get Data for Task info, Nodule info,nodule volume measures, and classification and flagged data."""
    rows = []
    data["Task ID"] = row["taskId"]
    data["Name"] = row["name"]
    if task.get("updatedBy"):
        data["Clinician Name"] = task.get("updatedBy")
    if task.get("updatedAt"):
        date = datetime.fromisoformat(task.get("updatedAt"))
        formatted_str = date.strftime('%Y:%m:%d %H:%M:%S')
        data["Updated At"] = formatted_str
    if row.get("currentStageName"):
        data["Stage"] = row.get("currentStageName")
    if task.get("status"):
        data["Status"] = task.get("status")
    if nodule.get("group"):
        data["Nodule Centroid"] = nodule.get("group")
    if nodule.get("attributes"):
        attributes = nodule.get("attributes")
        if attributes.get("Nodule Location"):
            data["Nodule Location"] = attributes.get("Nodule Location")
        if attributes.get("Nodule Type"):
            data["Nodule Type"] = attributes.get("Nodule Type")
        if attributes.get("Confidence on Nodule Type"):
            data["Confidence on Nodule Type"] = attributes.get("Confidence on Nodule Type")
        if attributes.get("Comments on Nodule Type"):
            data["Comments on Nodule Type"] = attributes.get("Comments on Nodule Type")
        if attributes.get("Nodule Morphology"):
            data["Nodule Morphology"] = attributes.get("Nodule Morphology")
        if attributes.get("Confidence on Nodule Morphology"):
            data["Confidence on Nodule Morphology"] = attributes.get("Confidence on Nodule Morphology")
        if attributes.get("Comments on Nodule Morphology"):
            data["Comments on Nodule Morphology"] = attributes.get("Comments on Nodule Morphology")                       
        if attributes.get("Nodule-wise LungRADS Score"):
            data["Nodule-wise LungRADS Score"] = attributes.get("Nodule-wise LungRADS Score")
        if attributes.get("Confidence on LungRADS Score"):
            data["Confidence on LungRADS Score"] = attributes.get("Confidence on LungRADS Score")
        if attributes.get("Comments on LungRADS Score"):
            data["Comments on LungRADS Score"] = attributes.get("Comments on LungRADS Score")
        if attributes.get("Nodule Suspicion Rank (1-5)"):
            data["Nodule Suspicion Rank (1-5)"] = attributes.get("Nodule Suspicion Rank (1-5)")
        if attributes.get("Entity Comments"):
            data["Entity Comments"] = attributes.get("Entity Comments")
        if (volume_measures) and (volume_measures != 0):
            group = nodule.get("group")
            for volume in volume_measures:           
                if volume.get("group") and volume["group"] == group:
                    if volume["category"] == "Nodule Volume 2D Min Diameter":
                        data["Nodule Volume 2D Min Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Volume 2D Max Diameter":
                        data["Nodule Volume 2D Max Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Volume 2D Mean Diameter":
                        data["Nodule Volume 2D Mean Diameter"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Min Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Max Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)
                    if volume["category"] == "Nodule Core 2D Mean Diameter (Only for part-solid nodules)":
                        data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"] = round(volume["length"], 4)
    
    if classification:
        attributes = classification.get("attributes")
        if attributes:
            if attributes.get("Study Reviewed?"):
                data["Classification (Study Reviewed?)"] = attributes.get("Study Reviewed?")
            if attributes.get("Case-wise LungRADS Score"):
                data["Classification (Case-wise LungRADS Score)"] = attributes.get("Case-wise LungRADS Score")
            if attributes.get("Confidence on LungRADS Score"):
                data["Classification (Confidence on LungRADS Score)"] = attributes.get("Confidence on LungRADS Score")
            if attributes.get("Comments on LungRADS Score"):
                data["Classification (Comments on LungRADS Score)"] = attributes.get("Comments on LungRADS Score")
    
    flagged_data = check_data_to_be_flagged(data)
    rows.append(flagged_data)
    return rows



def data_values():
    """The values needed to create a row"""
    data = {}
    data["Task ID"] = "----"
    data["Name"] = "----"
    data["Clinician Name"] = "----"
    data["Updated At"] = "----"
    data["Status"] = "----"
    data["Stage"] = "----"
    data["Nodule Centroid"] = "----"
    data["Nodule Location"] = "----"
    data["Nodule Type"] = "----"
    data["Confidence on Nodule Type"] = "----"
    data["Comments on Nodule Type"] = "----"
    data["Nodule Morphology"] = "----"
    data["Confidence on Nodule Morphology"] = "----"
    data["Comments on Nodule Morphology"] = "----"
    data["Nodule-wise LungRADS Score"] = "----"
    data["Confidence on LungRADS Score"] = "----"
    data["Comments on LungRADS Score"] = "----"
    data["Nodule Suspicion Rank (1-5)"] = "----"
    data["Entity Comments"] = "----"
    data["Nodule Volume 2D Mean Diameter"] = "----"
    data["Nodule Volume 2D Max Diameter"] = "----"
    data["Nodule Volume 2D Min Diameter"] = "----"
    data["Nodule Core 2D Mean Diameter (Only for part-solid nodules)"] = "----"
    data["Nodule Core 2D Max Diameter (Only for part-solid nodules)"] = "----"
    data["Nodule Core 2D Min Diameter (Only for part-solid nodules)"] = "----"
    data["Classification (Study Reviewed?)"] = "----"
    data["Classification (Case-wise LungRADS Score)"] = "----"
    data["Classification (Confidence on LungRADS Score)"] = "----"
    data["Classification (Comments on LungRADS Score)"] = "----"
    data["Flagged"] = ""
    return data


def check_rank(rank, datas):
    """Checks duplicates in Ranks"""
    new_rank = []
    for r in rank:
        if r != "----":
            new_rank.append(r)
    sets = list(set(new_rank))
    flagged = ""
    if len(new_rank) != len(sets):
        flagged = "Duplicate Ranks,"
    if len(new_rank) == 0:
        flagged = "Missing Attributes,"
    
    for data in datas:
        if data["Nodule Location"] != "----":
            if data["Nodule Suspicion Rank (1-5)"]  == "----" and len(sets) != 5:
                data["Flagged"] += "Missing Rank,"
        
            if data["Nodule Suspicion Rank (1-5)"]  != "----":
                data["Flagged"] += flagged



def check_if_task_has_consensus(row):
    """Check if Task has consensus"""
    print("****1111****")
    print(f"Checking if task has consensus for task: {row['taskId']}")
    super_truth = row.get("superTruth")
    consensus = row["consensusTasks"]
    rows = []
    data = data_values()
    if super_truth and type(super_truth) != float:
        nodules = super_truth['series'][0].get("landmarks3d")
        volume_measures = super_truth['series'][0].get("measurements")
        classification = super_truth.get("classification")
        print(len(nodules))
        if nodules and nodules != 0:
            nodule_rows = []
            ranks = []
            datas = []
            print(nodule_rows)
            print(datas)
            print(ranks)
            for nodule in nodules:
                print("****2222****")
                print(nodule)
                
                
                data = data_values()
                datas = get_task_data(row, super_truth, nodule, volume_measures, classification, data)
                ranks.append(datas[0]["Nodule Suspicion Rank (1-5)"])
                nodule_rows.extend(datas)
                
            print("****3333****")
            check_rank(ranks, nodule_rows)
            rows.extend(nodule_rows)
            
        else:
            data = data_values()
            datas = no_nodule(row, super_truth, classification, data)
            rows.extend(datas)

    if len(consensus) == 3:
        for task in consensus:
            nodules = task['series'][0].get("landmarks3d")
            volume_measures = task['series'][0].get("measurements")
            classification = task.get("classification")
            if nodules and nodules != 0:
                nodule_rows = []
                ranks = []
                datas = []
                for nodule in nodules:
                    data = data_values()
                    datas = get_task_data(row, task, nodule, volume_measures, classification, data)
                    ranks.append(datas[0]["Nodule Suspicion Rank (1-5)"])
                    nodule_rows.extend(datas)
                check_rank(ranks, nodule_rows)
                rows.extend(nodule_rows)
                
            else:
                data = data_values()
                datas = no_nodule(row, task, classification, data)
                rows.extend(datas)
    else:
        datas = empty_data(row)
        rows.extend(datas)
        
        
    return pd.DataFrame(rows)


def recreate_new_dataframe(df):
    """Recreate a new dataframe"""

    row = pd.concat(df.apply(check_if_task_has_consensus, axis=1).tolist(), ignore_index=True)
    return row

def create_a_data_frame(files):
    """This will create a DataFrame for JSON"""
    rows = []
    if not files:
        return pd.DataFrame()

    try:
        for file in files:
            data = {}
            data["name"] = file
            df = read_json_to_df(file)
            data["df"] = df
            rows.append(data)
        return rows
    except Exception as e:
        logging.error(f"Error creating DataFrame from files: {e}")
        return pd.DataFrame()
    
def find_json_datas(pattern):
    """Find Json Datas in the dir"""
    files = glob.glob(os.path.join(rds_dir, pattern))
    if not files:
        logging.warning(f"No File found matching '{pattern}'. Please check the path and file existence. Exiting.")
        return []
    else:
        return files


def find_json_blobs(bucket_name, prefix):
    """Find JSON blobs in GCS bucket/prefix."""
    if not bucket_name:
        return []

    client = storage.Client()
    blob_prefix = f"{prefix}/" if prefix else ""
    blobs = client.list_blobs(bucket_name, prefix=blob_prefix)
    json_paths = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith(".json")]
    if not json_paths:
        logging.warning(
            f"No JSON files found in gs://{bucket_name}/{blob_prefix}. "
            "Please check bucket/prefix and file existence."
        )
    return json_paths


def read_json_to_df(path):
    """Read JSON to DataFrame from local path or gs:// URI."""
    if path.startswith("gs://"):
        # gs://bucket/object-path
        no_scheme = path[len("gs://"):]
        bucket_name, blob_name = no_scheme.split("/", 1)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        content = blob.download_as_text()
        return pd.read_json(StringIO(content))
    return pd.read_json(path)


def build_output_filename(source_path):
    """Build output CSV name using project folder + date + source file name."""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    source_name = os.path.splitext(os.path.basename(source_path))[0]

    if PROJECT_FOLDER_NAME:
        project_folder = PROJECT_FOLDER_NAME
    else:
        normalized = source_path.replace("\\", "/").strip("/")
        parts = normalized.split("/")
        if source_path.startswith("gs://"):
            # gs://bucket/folder/file.json -> folder name if present
            project_folder = parts[-2] if len(parts) >= 4 else "project"
        else:
            # local: path/to/folder/file.json -> folder name if present
            project_folder = parts[-2] if len(parts) >= 2 else "project"

    return f"{project_folder}_{date_str}_{source_name}.csv"
    

def run_json():
    """This will run the JSON"""
    """start of execution"""
    if INPUT_BUCKET:
        files = find_json_blobs(INPUT_BUCKET, INPUT_PREFIX)
    else:
        files = find_json_datas("*.json")
    rows = create_a_data_frame(files)
    if len(rows) != 0:
        print("Final DataFrame created successfully:")
    else:
        print("Failed to create a valid DataFrame.")
    return rows


def main():
    if not OUTPUT_BUCKET:
        os.makedirs(csv_dir, exist_ok=True)

    rows = run_json()
    storage_client = storage.Client() if OUTPUT_BUCKET else None
    output_bucket = storage_client.bucket(OUTPUT_BUCKET) if OUTPUT_BUCKET else None

    for row in rows:
        filename = build_output_filename(row["name"])
        output_path = os.path.join(csv_dir, filename)

        full_df = row["df"]
        if full_df.empty:
            print(f"[WARN] No tasks found in file: {row['name']}")
            continue

        print(f"[INFO] Processing all tasks from file: {row['name']}")

        new_df = recreate_new_dataframe(full_df)
        if output_bucket:
            output_blob_name = f"{OUTPUT_PREFIX}/{filename}" if OUTPUT_PREFIX else filename
            output_blob = output_bucket.blob(output_blob_name)
            output_blob.upload_from_string(new_df.to_csv(index=False, sep=","), content_type="text/csv")
            print(f"[INFO] Uploaded CSV to gs://{OUTPUT_BUCKET}/{output_blob_name}")
        else:
            new_df.to_csv(output_path, index=False, sep=',')
            print(f"[INFO] Saved CSV to local path: {output_path}")


if __name__ == "__main__":
    main()