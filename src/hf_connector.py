from huggingface_hub import HfApi, HfFolder


def push_to_huggingface_repo(folder_path, repo_id, commit_message):
    """
    Push artifacts to a Hugging Face repository.

    Parameters:
    - file_path: str. The path to the JSONL file you want to upload.
    - repo_id: str. The repository ID on Hugging Face, usually in the format "username/repo_name".
    - commit_message: str. The commit message for the upload.
    """
    # Authenticate using the token from HfFolder (assumes you've already logged in with `huggingface-cli login`)
    token = HfFolder.get_token()
    if token is None:
        raise ValueError("You must be logged in to Hugging Face. Use `huggingface-cli login`.")

    # Upload the artifacts to the repository
    HfApi().upload_folder(
        token=token,
        repo_id=repo_id,
        folder_path=folder_path,
        commit_message=commit_message,
        repo_type='dataset'
    )
