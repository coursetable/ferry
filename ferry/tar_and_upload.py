import tarfile
import os.path

import argparse

from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth

from ferry import config

# tar a directory
def tar(directory, output_filename):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(directory, arcname=os.path.basename(directory))

# from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size,
# answer by Sridhar Ratnakumar

# get human readable file size


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Compress and upload directories to Google Drive")

    parser.add_argument(
        "-d",
        "--directories",
        nargs="+",
        help="Directories to process. Leave empty for all directories.",
        default=None,
        required=False,
    )

    parser.add_argument(
        "-u",
        "--upload",
        help="Whether or not to upload to the ferry-data directory (https://drive.google.com/drive/folders/14wl5ibpeLTQaVHK-DNTfLUaWb1N7lY7M?usp=sharing).",
        action='store_true'
    )

    args = parser.parse_args()

    directories_to_tar = args.directories

    # default directories
    if directories_to_tar is None:

        directories_to_tar = [
            f"{config.DATA_DIR}/course_evals",
            f"{config.DATA_DIR}/course_json_cache",
            f"{config.DATA_DIR}/migrated_courses",
            f"{config.DATA_DIR}/parsed_courses",
            f"{config.DATA_DIR}/previous_evals",
            f"{config.DATA_DIR}/previous_json",
            f"{config.DATA_DIR}/season_courses",
        ]

    for directory_path in directories_to_tar:

        if len(os.listdir(directory_path)) == 0:
            print(f"{directory_path} is empty, skipping")

            continue

        output_file = f"{directory_path}.tar.gz"
        tar(directory_path, output_file)

        print(f"Compressed {directory_path}. Output size: {sizeof_fmt(os.stat(output_file).st_size)}")

    if args.upload:

        # set up authentication
        print("Please authenticate Google account for uploading (Yale account required):")
        gauth = GoogleAuth(settings_file="./pydrive_settings.yaml")
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)

        parent_id = "14wl5ibpeLTQaVHK-DNTfLUaWb1N7lY7M"

        for directory_path in directories_to_tar:

            output_file = f"{directory_path}.tar.gz"
            uploaded_filename = output_file.split("/")[-1]

            updated = False

            # check if file already created
            file_list = drive.ListFile(
                {'q': f"'{parent_id}' in parents and trashed=false"}).GetList()

            # check if file is already present
            for file in file_list:
                if file['title'] == uploaded_filename:

                    file.SetContentFile(output_file)
                    file.Upload()

                    print(f"{uploaded_filename} already present, updating.")

                    updated = True

            if not updated:

                    # if not yet created, make and upload
                file = drive.CreateFile({
                    'title': uploaded_filename,
                    'parents': [{'id': parent_id, "kind": "drive#fileLink"}]
                })

                file.SetContentFile(output_file)
                file.Upload()

                print(f"Created {output_file}")
