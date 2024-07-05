# Monocorpus: Tatar Language Monocorpus Development Tools

## Overview

The Monocorpus project aims to provide tools for developing a Tatar language monocorpus. The project includes
functionality to extract texts from books and save them in files.

## Features
- Extract text from EPUB and PDF files.
- Post-processing of extracted text to remove unwanted characters (e.g. OCR artifacts). More precisely, the following steps are performed: 
  - Remove sudden ASCII chars in the tatar word (e.g. **с**[0x0063)]у --> **с**[0x0441]у) 
  - Remove sudden non-ASCII chars in the non-tatar word (e.g. **а**[0x0430]rm --> **a**[0x0061]rm)
  - Unify punctuation marks by replacing look-alikes with a single variant (e.g. '»' | '«' | '“' | '”' | '„' --> '"')
  - Remove unwanted characters (e.g. '•') 
  - Remove sudden digits at the end of the word (e.g. башына2 —> башына)
  
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

3. **Extract Texts from Books:**

- Place your book(s) into the `workdir/000_entry_point` folder. Currently we support EPUB and PDF formats.
- Run the script to extract texts:

```
python src/main.py extract
```

4. **Proces dirty extracted texts further:**

- Run the script to process extracted texts:
```
python src/main.py process
```

5. **Explore the Output:**

- Processed text files will be saved in the `workdir/900_artifacts` directory.

## Project Structure

- **`src/`:** Contains the main script for text extraction and processing.
- **`workdir/000_entry_point`:** Place your books here for text extraction.
- **`workdir/900_artifacts`:** Processed text files will be saved here.
- **`requirements.txt`:** List of required Python dependencies.

## Contributing

Contributions are welcome! If you'd like to contribute to the project, make your changes and submit a pull request
detailing the changes made.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
