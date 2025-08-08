import os
import sys
import requests
from zipfile import ZipFile

def download_zip_file(url, output_dir):
    """Download zip file from given URL"""
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code {response.status_code}")

def extract_zip_file(zip_filename, output_dir):
    """Extract contents of zip file"""
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    print(f"Extracted files written to: {output_dir}")
    print("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir):
    """Convert JSON to newline-delimited format"""
    import json
    input_path = os.path.join(output_dir, "dict_artists.json")
    output_path = os.path.join(output_dir, "fixed_da.json")
    
    with open(input_path, "r") as f_in, open(output_path, "w", encoding="utf-8") as f_out:
        data = json.load(f_in)
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
    
    print(f"File {input_path} has been fixed and written to {output_path}")
    print("Removing the original file")
    os.remove(input_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Example Usage:")
        print("python3 execute.py /path/to/extract/directory")
        sys.exit(1)
    
    try:
        print("Starting Extraction Engine...")
        EXTRACT_PATH = sys.argv[1]
        
        # Your Kaggle URL goes here
        KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250729%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250729T124405Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=559ff5f87d2020979df66fde0b1818391c08e8fb74c234b65b6b9aa9fc59155cf5e32cfa76f3c6b78e9e9a1c6c0406868679152c58ea6ebd7029f40684dc49ceb5eab8a2354b644a4d375eba0174914a1714eb93b4610c734a9c669d762a62ea5c8aa5bf4504113e859e5f15303280c6d211106e0511e29487ee2f4fe36f54875c0e3ee453795941016c26238fb96b4aab9d0e9a5cf4324d70845bf2fc835df31c1d756c4271ea498c09bd60a15a48d4f0e9c32edda17ef80c1271f571105d9425939a68fb049bc90e7eba1e74110a95634f544d310ab2834cbcbac45ba91f3e10ec7d63f9927ad91276546a75adcec1ace3e5384fceb09efdf72d6c333ea32d"
        
        # Execute pipeline steps
        zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
        extract_zip_file(zip_filename, EXTRACT_PATH)
        fix_json_dict(EXTRACT_PATH)
        
        print("Extraction Successfully Completed")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)