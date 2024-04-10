# Monocorpus: Tatar Language Monocorpus Development Tools


## Overview

The Monocorpus project aims to provide tools for developing a Tatar language monocorpus. The project includes functionality to extract texts from books and save them in files.

## Getting Started

To get started with the project, follow these steps:

1. **Clone the Repository:**
   ```
   git clone https://github.com/neurotatarlar/monocorpus.git
   cd monocorpus
   ```

2. **Prepare Python Environment:**
   - Make sure you have Python 3.x installed on your system.
   - Create and activate a virtual environment (optional but recommended):
     ```
     python3 -m venv venv
     source venv/bin/activate
     ```
   - Install the required dependencies:
     ```
     pip install -r requirements.txt
     ```

3. **Process Texts from Books:**
   - Place your book(s) into the `workdir/000_entry_point` folder. Currently we support EPUB and PDF formats.
   - Run the script to extract and process texts:
     ```
     python src/main.py
     ```

4. **Explore the Output:**
   - Processed text files will be saved in the `workdir/900_artifacts` directory.

5. **Push the resulting file (probably in jsonl format):**
   - Log in using command and paste the token for your account: 
   ```
   huggingface-cli login
   ```
   - Put your credentials to the `src/config.ini` file and start the script from `src` folder: 
   ```
   python hf_connector.py 
   ```
   or you could provide the credentials using cli args:
   ```
   python hf_connector.py --file_path path_to_file.jsonl --repo_id username/repo_name
   ```   

## Project Structure

- **`src/`:** Contains the main script for text extraction and processing.
- **`workdir/000_entry_point`:** Place your books here for text extraction.
- **`workdir/900_artifacts`:** Processed text files will be saved here.
- **`requirements.txt`:** List of required Python dependencies.

## Contributing

Contributions are welcome! If you'd like to contribute to the project, make your changes and submit a pull request detailing the changes made.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
