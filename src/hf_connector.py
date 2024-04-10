import argparse
import configparser
from huggingface_hub import HfApi, HfFolder

def push_to_huggingface_repo(file_path, repo_id, commit_message="Updating the dataset file"):
    """
    Push a file to a Hugging Face repository.

    Parameters:
    - file_path: str. The path to the JSONL file you want to upload.
    - repo_id: str. The repository ID on Hugging Face, usually in the format "username/repo_name".
    - commit_message: str. The commit message for the upload.
    """
    # Authenticate using the token from HfFolder (assumes you've already logged in with `huggingface-cli login`)
    token = HfFolder.get_token()
    if token is None:
        raise ValueError("You must be logged in to Hugging Face. Use `huggingface-cli login`.")

    # Initialize the HfApi instance
    api = HfApi()

    # Check if the file already exists in the repository
    repo_files = api.list_repo_files(repo_id, token=token)
    if any(file['name'] == file_path for file in repo_files):
        # If the file exists, delete it
        api.delete_file(repo_id, file_path, token=token, commit_message=f"Delete {file_path}, so we could repalce it by updated version of the file")

    # Upload the new file to the repository
    api.upload_file(
        token=token,
        repo_id=repo_id,
        path_or_fileobj=file_path,
        path_in_repo=file_path,  # This sets the path in the repository. Adjust if needed.
        commit_message=commit_message
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Push a file to a Hugging Face repository.')
    parser.add_argument('--file_path', type=str, help='The path to the file you want to upload.')
    parser.add_argument('--repo_id', type=str, help='The repository ID on Hugging Face, in the format "username/repo_name".')
    args = parser.parse_args()

    # If no arguments were provided, try to read them from the config file
    if args.file_path is None or args.repo_id is None:
        config = configparser.ConfigParser()
        config.read('config.ini')

        file_path = config.get('DEFAULT', 'file_path')
        repo_id = config.get('DEFAULT', 'repo_id')
    else:
        file_path = args.file_path
        repo_id = args.repo_id

    push_to_huggingface_repo(file_path, repo_id)