from crossref.restful import Works

def _parse_document_type(self, doc):
        """ Returns the document type of a document as a <class 'str'>.
        e.g. 'paper', 'clinical_trial', 'patent', 'news', 'chapter'. """
        
        doi = self._parse_doi(self, doc)
        
        works = Works()
        doc_type = works.doi(doi)
        
        if doc_type == 'book-chapter':
            return 'chapter'
        else:
            return 'paper'
        