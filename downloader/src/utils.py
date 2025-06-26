from pathlib import Path


def cleanup_files_from_dir(dir_path):
    for file in dir_path.iterdir():
        if file.is_file():
            file.unlink()
