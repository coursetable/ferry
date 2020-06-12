import tarfile
import os.path

import argparse

from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth

from ferry import config

# tar a directory


def untar(tar_filename):
    with tarfile.open(tar_filename, "r:gz") as tar:
        tar.extractall(path=config.DATA_DIR)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download and uncompress directories from Google Drive")

    parser.add_argument(
        "-e",
        "--extract",
        help="Whether or not to extract the downloaded tar archives. False by default.",
        action='store_true'
    )

    args = parser.parse_args()

    # set up authentication
    print("Please authenticate Google account for uploading (Yale account required):")
    gauth = GoogleAuth(settings_file="./pydrive_settings.yaml")
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    parent_id = "14wl5ibpeLTQaVHK-DNTfLUaWb1N7lY7M"

    # get files in directory
    file_list = drive.ListFile(
        {'q': f"'{parent_id}' in parents and trashed=false"}).GetList()

    # download each file in directory
    for file in file_list:
        print(f"Downloading {file['title']}")

        file.GetContentFile(f"{config.DATA_DIR}/{file['title']}")

        if args.extract:

            print(f"Extracting {file['title']}")

            untar(f"{config.DATA_DIR}/{file['title']}")
