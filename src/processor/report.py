class ProcessingReport:
    """
    Class to store the report of the processing of the files
    """

    def __init__(self):
        self._processed_files = 0
        self._not_documents = []
        self._not_supported_yet = []
        self._extracted_docs = []
        self._already_extracted = []

    def __str__(self):
        return (
            "====================================================\n"
            f"Overall report: {self._processed_files} file(s) was processed\n"
            f"{len(self._extracted_docs)} file(s) that text was extracted from: {self._extracted_docs},\n"
            f"{len(self._not_documents)} file(s) is not a document(s): {self._not_documents},\n"
            f"{len(self._not_supported_yet)} file(s) has unsupported yet format: {self._not_supported_yet},\n"
            f"{len(self._already_extracted)} file(s) was already extracted: {self._already_extracted}"
        )

    def not_a_document(self, file_name: str):
        self._processed_files += 1
        self._not_documents.append(file_name)

    def not_supported_yet(self, file_name: str):
        self._processed_files += 1
        self._not_supported_yet.append(file_name)

    def extracted_doc(self, file_name: str):
        self._processed_files += 1
        self._extracted_docs.append(file_name)

    def already_extracted(self, file_name: str):
        self._processed_files += 1
        self._already_extracted.append(file_name)
